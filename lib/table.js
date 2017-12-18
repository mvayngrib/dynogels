'use strict';

const co = require('co').wrap;
const Promise = require('bluebird');
const _ = require('lodash');
const Item = require('./item');
const Query = require('./query');
const Scan = require('./scan');
const EventEmitter = require('events').EventEmitter;
const utils = require('./utils');
const ParallelScan = require('./parallelScan');
const expressions = require('./expressions');
const Autoscaler = require('./autoscale');

const internals = {};

const Table = module.exports = function (name, schema, serializer, docClient, logger) {
  this.config = { name: name };
  this.schema = schema;
  this.serializer = serializer;
  this.docClient = docClient;
  this.log = logger;

  this._before = new EventEmitter();
  this.before = this._before.on.bind(this._before);

  this._after = new EventEmitter();
  this.after = this._after.on.bind(this._after);
};

Table.prototype.initItem = function (attrs) {
  if (this.itemFactory) {
    return new this.itemFactory(attrs);
  }

  return new Item(attrs, this);
};

Table.prototype.tableName = function () {
  if (this.schema.tableName) {
    if (_.isFunction(this.schema.tableName)) {
      return this.schema.tableName.call(this);
    }

    return this.schema.tableName;
  }

  return this.config.name;
};

Table.prototype.sendRequest = co(function* (method, params) {
  let driver;
  if (_.isFunction(this.docClient[method])) {
    driver = this.docClient;
  } else if (_.isFunction(this.docClient.service[method])) {
    driver = this.docClient.service;
  }

  const startTime = Date.now();

  this.log.info({ params: params }, 'dynogels %s request', method.toUpperCase());
  try {
    const data = yield driver[method](params).promise();
    this.log.info({ data }, 'dynogels %s response - %sms', method.toUpperCase(), Date.now() - startTime);
    return data
  } catch (err) {
    this.log.warn({ err }, 'dynogels %s error', method.toUpperCase());
    throw err
  }
});

Table.prototype.get = co(function* (hashKey, rangeKey, options={}) {
  if (_.isPlainObject(rangeKey)) {
    options = rangeKey;
    rangeKey = undefined;
  }

  let params = {
    TableName: this.tableName(),
    Key: this.serializer.buildKey(hashKey, rangeKey, this.schema)
  };

  params = _.merge({}, params, options);

  const { Item } = yield this.sendRequest('get', params)
  return Item && this.initItem(this.serializer.deserializeItem(Item))
});

internals.callBeforeHooks = co(function* (table, name, startFun) {
  const listeners = table._before.listeners(name);
  return yield utils.waterfall([startFun].concat(listeners))
});

Table.prototype.create = co(function* (item, options={}) {
  if (_.isArray(item)) {
    return yield Promise.map(item, data => internals.createItem(this, data, options))
  }

  return yield internals.createItem(this, item, options)
})

internals.createItem = co(function* (table, item, options) {
  const start = co(function* () {
    const data = table.schema.applyDefaults(item);
    const paramName = _.isString(table.schema.createdAt) ? table.schema.createdAt : 'createdAt';
    if (table.schema.timestamps && table.schema.createdAt !== false && !_.has(data, paramName)) {
      data[paramName] = new Date().toISOString();
    }

    return data
  }).bind(this);

  const data = yield internals.callBeforeHooks(table, 'create', start)
  const result = table.schema.validate(data);
  if (result.error) {
    result.error.message = `${result.error.message} on ${table.tableName()}`;
    throw result.error
  }

  const attrs = utils.omitNulls(data);

  let params = {
    TableName: table.tableName(),
    Item: table.serializer.serializeItem(table.schema, attrs)
  };

  if (options.expected) {
    internals.addConditionExpression(params, options.expected);
    options = _.omit(options, 'expected');
  }

  if (options.overwrite === false) {
    const expected = _.chain([table.schema.hashKey, table.schema.rangeKey]).compact().reduce((result, key) => {
      _.set(result, `${key}.<>`, _.get(params.Item, key));
      return result;
    }, {}).value();

    internals.addConditionExpression(params, expected);
  }

  options = _.omit(options, 'overwrite'); // remove overwrite flag regardless if true or false

  params = _.merge({}, params, options);

  yield table.sendRequest('put', params)
  const initializedItem = table.initItem(attrs);
  table._after.emit('create', initializedItem);
  return initializedItem
});

internals.updateExpressions = (schema, data, options) => {
  const exp = expressions.serializeUpdateExpression(schema, data);

  if (options.UpdateExpression) {
    const parsed = expressions.parse(options.UpdateExpression);

    exp.expressions = _.reduce(parsed, (result, val, key) => {
      if (!_.isEmpty(val)) {
        result[key] = result[key].concat(val);
      }

      return result;
    }, exp.expressions);
  }

  if (_.isPlainObject(options.ExpressionAttributeValues)) {
    exp.values = _.merge({}, exp.values, options.ExpressionAttributeValues);
  }

  if (_.isPlainObject(options.ExpressionAttributeNames)) {
    exp.attributeNames = _.merge({}, exp.attributeNames, options.ExpressionAttributeNames);
  }

  return _.merge({}, {
    ExpressionAttributeValues: exp.values,
    ExpressionAttributeNames: exp.attributeNames,
    UpdateExpression: expressions.stringify(exp.expressions),
  });
};

internals.validateItemFragment = (item, schema) => {
  const result = {};
  const error = {};

  // get the list of attributes to remove
  const removeAttributes = _.pickBy(item, _.isNull);

  // get the list of attributes whose value is an object
  const setOperationValues = _.pickBy(item, i => _.isPlainObject(i) && (i.$add || i.$del));

  // get the list of attributes to modify
  const updateAttributes = _.omit(
    item,
    Object.keys(removeAttributes).concat(Object.keys(setOperationValues))
  );

  // check attribute removals for .required() schema violation
  const removalValidation = schema.validate(
    {},
    { abortEarly: false }
  );

  if (removalValidation.error) {
    const errors = _.pickBy(
      removalValidation.error.details,
      e => _.isEqual(e.type, 'any.required')
      && Object.prototype.hasOwnProperty.call(removeAttributes, e.path)
    );
    if (!_.isEmpty(errors)) {
      error.remove = errors;
      result.error = error;
    }
  }

  // check attribute updates match the schema
  const updateValidation = schema.validate(
    updateAttributes,
    { abortEarly: false }
  );

  if (updateValidation.error) {
    const errors = _.omitBy(
      updateValidation.error.details,
      e => _.isEqual(e.type, 'any.required')
    );
    if (!_.isEmpty(errors)) {
      error.update = errors;
      result.error = error;
    }
  }

  return result;
};

Table.prototype.update = co(function* (item, options={}) {
  const schemaValidation = internals.validateItemFragment(item, this.schema);
  if (schemaValidation.error) {
    throw _.assign(new Error(`Schema validation error while updating item in table ${this.tableName()}: ${JSON.stringify(schemaValidation.error)}`), {
      name: 'DynogelsUpdateError',
      detail: schemaValidation.error
    });
  }

  const start = co(function* () {
    const paramName = _.isString(this.schema.updatedAt) ? this.schema.updatedAt : 'updatedAt';

    if (this.schema.timestamps && this.schema.updatedAt !== false && !_.has(item, paramName)) {
      item[paramName] = new Date().toISOString();
    }

    return item
  }).bind(this);

  const data = yield internals.callBeforeHooks(this, 'update', start)
  const hashKey = data[this.schema.hashKey];
  let rangeKey = data[this.schema.rangeKey];

  if (_.isUndefined(rangeKey)) {
    rangeKey = undefined;
  }

  let params = {
    TableName: this.tableName(),
    Key: this.serializer.buildKey(hashKey, rangeKey, this.schema),
    ReturnValues: 'ALL_NEW'
  };

  const exp = internals.updateExpressions(this.schema, data, options);
  params = _.assign(params, exp);

  if (options.expected) {
    internals.addConditionExpression(params, options.expected);
  }

  const unprocessedOptions = _.omit(options, ['UpdateExpression', 'ExpressionAttributeValues', 'ExpressionAttributeNames', 'expected']);

  params = _.chain({}).merge(params, unprocessedOptions).omitBy(_.isEmpty).value();
  const { Attributes } = yield this.sendRequest('update', params)
  const result = Attributes && this.initItem(this.serializer.deserializeItem(Attributes));
  this._after.emit('update', result);
  return result
});

internals.addConditionExpression = (params, expectedConditions) => {
  _.each(expectedConditions, (val, key) => {
    let operator;
    let expectedValue = null;

    const existingValueKeys = _.keys(params.ExpressionAttributeValues);

    if (_.isObject(val) && _.isBoolean(val.Exists) && val.Exists === true) {
      operator = 'attribute_exists';
    } else if (_.isObject(val) && _.isBoolean(val.Exists) && val.Exists === false) {
      operator = 'attribute_not_exists';
    } else if (_.isObject(val) && _.has(val, '<>')) {
      operator = '<>';
      expectedValue = _.get(val, '<>');
    } else {
      operator = '=';
      expectedValue = val;
    }

    const condition = expressions.buildFilterExpression(key, operator, existingValueKeys, expectedValue, null);
    params.ExpressionAttributeNames = _.merge({}, condition.attributeNames, params.ExpressionAttributeNames);
    params.ExpressionAttributeValues = _.merge({}, condition.attributeValues, params.ExpressionAttributeValues);

    if (_.isString(params.ConditionExpression)) {
      params.ConditionExpression = `${params.ConditionExpression} AND (${condition.statement})`;
    } else {
      params.ConditionExpression = `(${condition.statement})`;
    }
  });
};

Table.prototype.destroy = co(function* (hashKey, rangeKey, options={}) {
  if (_.isPlainObject(rangeKey)) {
    options = rangeKey;
    rangeKey = undefined;
  }

  if (_.isPlainObject(hashKey)) {
    rangeKey = hashKey[this.schema.rangeKey];

    if (_.isUndefined(rangeKey)) {
      rangeKey = undefined;
    }

    hashKey = hashKey[this.schema.hashKey];
  }

  let params = {
    TableName: this.tableName(),
    Key: this.serializer.buildKey(hashKey, rangeKey, this.schema)
  };

  if (options.expected) {
    internals.addConditionExpression(params, options.expected);

    delete options.expected;
  }

  params = _.merge({}, params, options);

  const { Attributes } = yield this.sendRequest('delete', params)
  const item = Attributes && this.initItem(this.serializer.deserializeItem(Attributes));
  this._after.emit('destroy', item);
  return item
});

Table.prototype.query = function (hashKey) {
  return new Query(hashKey, this, this.serializer);
};

Table.prototype.scan = function () {
  return new Scan(this, this.serializer);
};

Table.prototype.parallelScan = function (totalSegments) {
  return new ParallelScan(this, this.serializer, totalSegments);
};

Table.prototype.autoscale = function (options) {
  return new Autoscaler(_.extend({ table: this }, options));
};

internals.deserializeItems = (table, data) => {
  const result = {};
  if (data.Items) {
    result.Items = _.map(data.Items, i => table.initItem(table.serializer.deserializeItem(i)));

    delete data.Items;
  }

  if (data.LastEvaluatedKey) {
    result.LastEvaluatedKey = data.LastEvaluatedKey;

    delete data.LastEvaluatedKey;
  }

  return _.merge({}, data, result)
}

Table.prototype.runQuery = co(function* (params) {
  const data = yield this.sendRequest('query', params)
  return internals.deserializeItems(this, data)
});

Table.prototype.runScan = co(function* (params) {
  const data = yield this.sendRequest('scan', params)
  return internals.deserializeItems(this, data)
});

Table.prototype.runBatchGetItems = co(function* (params) {
  return yield this.sendRequest('batchGet', params);
});

internals.attributeDefinition = (schema, key) => {
  let type = schema._modelDatatypes[key];

  if (type === 'DATE') {
    type = 'S';
  }

  return {
    AttributeName: key,
    AttributeType: type
  };
};

internals.keySchema = (hashKey, rangeKey) => {
  const result = [{
    AttributeName: hashKey,
    KeyType: 'HASH'
  }];

  if (rangeKey) {
    result.push({
      AttributeName: rangeKey,
      KeyType: 'RANGE'
    });
  }

  return result;
};

internals.secondaryIndex = (schema, params) => {
  const projection = params.projection || { ProjectionType: 'ALL' };

  return {
    IndexName: params.name,
    KeySchema: internals.keySchema(schema.hashKey, params.rangeKey),
    Projection: projection
  };
};

internals.globalIndex = (indexName, params) => {
  const projection = params.projection || { ProjectionType: 'ALL' };

  return {
    IndexName: indexName,
    KeySchema: internals.keySchema(params.hashKey, params.rangeKey),
    Projection: projection,
    ProvisionedThroughput: {
      ReadCapacityUnits: params.readCapacity || 1,
      WriteCapacityUnits: params.writeCapacity || 1
    }
  };
};

Table.prototype.createTable = co(function* (options={}) {
  const attributeDefinitions = [];
  attributeDefinitions.push(internals.attributeDefinition(this.schema, this.schema.hashKey));

  if (this.schema.rangeKey) {
    attributeDefinitions.push(internals.attributeDefinition(this.schema, this.schema.rangeKey));
  }

  const localSecondaryIndexes = [];

  _.forEach(this.schema.secondaryIndexes, params => {
    attributeDefinitions.push(internals.attributeDefinition(this.schema, params.rangeKey));
    localSecondaryIndexes.push(internals.secondaryIndex(this.schema, params));
  });

  const globalSecondaryIndexes = [];

  _.forEach(this.schema.globalIndexes, (params, indexName) => {
    if (!_.find(attributeDefinitions, { AttributeName: params.hashKey })) {
      attributeDefinitions.push(internals.attributeDefinition(this.schema, params.hashKey));
    }

    if (params.rangeKey && !_.find(attributeDefinitions, { AttributeName: params.rangeKey })) {
      attributeDefinitions.push(internals.attributeDefinition(this.schema, params.rangeKey));
    }

    globalSecondaryIndexes.push(internals.globalIndex(indexName, params));
  });

  const keySchema = internals.keySchema(this.schema.hashKey, this.schema.rangeKey);
  const params = {
    AttributeDefinitions: attributeDefinitions,
    TableName: this.tableName(),
    KeySchema: keySchema,
    ProvisionedThroughput: {
      ReadCapacityUnits: options.readCapacity || 1,
      WriteCapacityUnits: options.writeCapacity || 1
    }
  };

  if (localSecondaryIndexes.length >= 1) {
    params.LocalSecondaryIndexes = localSecondaryIndexes;
  }

  if (globalSecondaryIndexes.length >= 1) {
    params.GlobalSecondaryIndexes = globalSecondaryIndexes;
  }

  if (options.hasOwnProperty('streamSpecification') && typeof options.streamSpecification === 'object') {
    params.StreamSpecification = {
      StreamEnabled: options.streamSpecification.streamEnabled || false
    };
    if (params.StreamSpecification.StreamEnabled) {
      params.StreamSpecification.StreamViewType = options.streamSpecification.streamViewType || 'NEW_AND_OLD_IMAGES';
    }
  }

  return yield this.sendRequest('createTable', params);
});

Table.prototype.awaitExists = co(function* () {
  const params = {
    TableName: this.tableName(),
  };

  return yield this.docClient.service.waitFor('tableExists', params).promise();
});

Table.prototype.describeTable = co(function* () {
  const params = {
    TableName: this.tableName(),
  };

  return yield this.sendRequest('describeTable', params);
});

Table.prototype.deleteTable = co(function* () {
  const params = {
    TableName: this.tableName(),
  };

  return yield this.sendRequest('deleteTable', params);
});

Table.prototype.updateTable = co(function* (throughput={}) {
  yield Promise.all([
    internals.syncIndexes(this),
    internals.updateTableCapacity(this, throughput)
  ])
});

internals.updateTableCapacity = co(function* (table, throughput) {
  const params = {};

  if (_.has(throughput, 'readCapacity') || _.has(throughput, 'writeCapacity')) {
    params.ProvisionedThroughput = {};

    if (_.has(throughput, 'readCapacity')) {
      params.ProvisionedThroughput.ReadCapacityUnits = throughput.readCapacity;
    }

    if (_.has(throughput, 'writeCapacity')) {
      params.ProvisionedThroughput.WriteCapacityUnits = throughput.writeCapacity;
    }
  }

  if (!_.isEmpty(params)) {
    params.TableName = table.tableName();
    return yield table.sendRequest('updateTable', params);
  }
});

internals.syncIndexes = co(function* (table) {
  const data = yield table.describeTable()
  const missing = _.values(internals.findMissingGlobalIndexes(table, data));
  if (_.isEmpty(missing)) return

  // UpdateTable only allows one new index per UpdateTable call
  // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.OnlineOps.html#GSI.OnlineOps.Creating
  const maxIndexCreationsAtaTime = 5;
  return yield Promise.map(missing, co(function* (params) {
    const attributeDefinitions = [];

    if (!_.find(attributeDefinitions, { AttributeName: params.hashKey })) {
      attributeDefinitions.push(internals.attributeDefinition(table.schema, params.hashKey));
    }

    if (params.rangeKey && !_.find(attributeDefinitions, { AttributeName: params.rangeKey })) {
      attributeDefinitions.push(internals.attributeDefinition(table.schema, params.rangeKey));
    }

    const currentWriteThroughput = data.Table.ProvisionedThroughput.WriteCapacityUnits;
    const newIndexWriteThroughput = _.ceil(currentWriteThroughput * 1.5);
    params.writeCapacity = params.writeCapacity || newIndexWriteThroughput;

    table.log.info('adding index %s to table %s', params.name, table.tableName());

    const updateParams = {
      TableName: table.tableName(),
      AttributeDefinitions: attributeDefinitions,
      GlobalSecondaryIndexUpdates: [{ Create: internals.globalIndex(params.name, params) }]
    };

    return yield table.sendRequest('updateTable', updateParams);
  }), { concurrency: maxIndexCreationsAtaTime });
});

internals.findMissingGlobalIndexes = (table, data) => {
  if (_.isNull(data) || _.isUndefined(data)) {
    // table does not exist
    return table.schema.globalIndexes;
  } else {
    const indexData = _.get(data, 'Table.GlobalSecondaryIndexes');
    const existingIndexNames = _.map(indexData, 'IndexName');

    const missing = _.reduce(table.schema.globalIndexes, (result, idx, indexName) => {
      if (!_.includes(existingIndexNames, idx.name)) {
        result[indexName] = idx;
      }

      return result;
    }, {});

    return missing;
  }
};
