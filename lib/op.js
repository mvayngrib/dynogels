const co = require('co').wrap
const _ = require('lodash');
const internals = require('./op-internals');

const Op = module.exports = function (table, serializer) {
  this.table = table;
  this.serializer = serializer;
};

const proto = Op.prototype;

proto.limit = function (num) {
  if (num <= 0) {
    throw new Error('Limit must be greater than 0');
  }

  this.request.Limit = num;

  return this;
};

proto.loadAll = function () {
  this.options.loadAll = true;

  return this;
};

proto.startKey = function (hashKey, rangeKey) {
  this.request.ExclusiveStartKey = this.serializer.buildKey(hashKey, rangeKey, this.table.schema);

  return this;
};

proto.clearStartKey = function () {
  delete this.request.ExclusiveStartKey;
};

proto.canContinue = function () {
  return !!this.position();
};

proto.position = function () {
  return this.request.ExclusiveStartKey;
};

proto.consistentRead = function (read) {
  if (!_.isBoolean(read)) {
    read = true;
  }

  this.request.ConsistentRead = read;

  return this;
};

proto.continue = co(function* () {
  if (!this.canContinue()) {
    throw new Error('cannot continue');
  }

  return this.exec();
});

proto.attributes = function (attrs) {
  if (!_.isArray(attrs)) {
    attrs = [attrs];
  }

  const expressionAttributeNames = _.reduce(attrs, (result, attr) => {
    const path = `#${attr}`;
    result[path] = attr;

    return result;
  }, {});

  this.request.ProjectionExpression = _.keys(expressionAttributeNames).join(',');
  this.request.ExpressionAttributeNames = _.merge({}, expressionAttributeNames, this.request.ExpressionAttributeNames);

  return this;
};

proto.select = function (value) {
  this.request.Select = value;

  return this;
};

proto.filterExpression = function (expression) {
  this.request.FilterExpression = expression;

  return this;
};

proto.expressionAttributeValues = function (data) {
  this.request.ExpressionAttributeValues = data;

  return this;
};

proto.expressionAttributeNames = function (data) {
  this.request.ExpressionAttributeNames = data;

  return this;
};

proto.projectionExpression = function (data) {
  this.request.ProjectionExpression = data;

  return this;
};

proto.returnConsumedCapacity = function (value) {
  if (_.isUndefined(value)) {
    value = 'TOTAL';
  }

  this.request.ReturnConsumedCapacity = value;

  return this;
};

proto.buildRequest = function () {
  return _.merge({}, this.request, { TableName: this.table.tableName() });
};

proto.addFilterCondition = function (condition) {
  internals.addExpressionAttributes(this.request, condition);
  internals.updateFilterExpression(this.request, condition);
  return this;
};
