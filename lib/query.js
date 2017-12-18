'use strict';

const co = require('co').wrap
const inherits = require('inherits');
const _ = require('lodash');
const expressions = require('./expressions');
const utils = require('./utils');
const Op = require('./op');
const commonInternals = require('./op-internals');
const internals = _.clone(commonInternals);

internals.keyCondition = (keyName, schema, query) => {
  const f = operator => function () {
    const copy = [].slice.call(arguments);
    const existingValueKeys = _.keys(query.request.ExpressionAttributeValues);
    const args = [keyName, operator, existingValueKeys].concat(copy);
    const cond = expressions.buildFilterExpression.apply(null, args);
    return query.addKeyCondition(cond);
  };

  return {
    equals: f('='),
    eq: f('='),
    lte: f('<='),
    lt: f('<'),
    gte: f('>='),
    gt: f('>'),
    beginsWith: f('begins_with'),
    between: f('BETWEEN')
  };
};

internals.queryFilter = (keyName, schema, query) => {
  const f = operator => function () {
    const copy = [].slice.call(arguments);
    const existingValueKeys = _.keys(query.request.ExpressionAttributeValues);
    const args = [keyName, operator, existingValueKeys].concat(copy);
    const cond = expressions.buildFilterExpression.apply(null, args);
    return query.addFilterCondition(cond);
  };

  return {
    equals: f('='),
    eq: f('='),
    ne: f('<>'),
    lte: f('<='),
    lt: f('<'),
    gte: f('>='),
    gt: f('>'),
    null: f('attribute_not_exists'),
    exists: f('attribute_exists'),
    contains: f('contains'),
    notContains: f('NOT contains'),
    in: f('IN'),
    beginsWith: f('begins_with'),
    between: f('BETWEEN')
  };
};

internals.isUsingGlobalIndex = query => query.request.IndexName && query.table.schema.globalIndexes[query.request.IndexName];

const Query = module.exports = function (hashKey, table, serializer) {
  Op.call(this, table, serializer);

  this.hashKey = hashKey;
  this.options = { loadAll: false };
  this.request = {};
};

inherits(Query, Op);

Query.prototype.usingIndex = function (name) {
  this.request.IndexName = name;

  return this;
};

Query.prototype.addKeyCondition = function (condition) {
  internals.addExpressionAttributes(this.request, condition);

  if (_.isString(this.request.KeyConditionExpression)) {
    this.request.KeyConditionExpression = `${this.request.KeyConditionExpression} AND (${condition.statement})`;
  } else {
    this.request.KeyConditionExpression = `(${condition.statement})`;
  }

  return this;
};

Query.prototype.ascending = function () {
  this.request.ScanIndexForward = true;

  return this;
};

Query.prototype.descending = function () {
  this.request.ScanIndexForward = false;

  return this;
};

Query.prototype.where = function (keyName) {
  return internals.keyCondition(keyName, this.table.schema, this);
};

Query.prototype.filter = function (keyName) {
  return internals.queryFilter(keyName, this.table.schema, this);
};

Query.prototype.exec = co(function* () {
  if (!this._executed) {
    // allow pagination by re-executing
    this._executed = true;
    this.addKeyCondition(this.buildKey());
  }

  const runQuery = params => this.table.runQuery(params)
  return yield utils.paginatedRequest(this, runQuery);
});

Query.prototype.buildKey = function () {
  let key = this.table.schema.hashKey;

  if (internals.isUsingGlobalIndex(this)) {
    key = this.table.schema.globalIndexes[this.request.IndexName].hashKey;
  }

  const existingValueKeys = _.keys(this.request.ExpressionAttributeValues);
  return expressions.buildFilterExpression(key, '=', existingValueKeys, this.hashKey);
};
