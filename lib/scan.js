'use strict';

const co = require('co').wrap
const inherits = require('inherits');
const _ = require('lodash');
const expressions = require('./expressions');
const utils = require('./utils');
const Op = require('./op');
const internals = {};

internals.keyCondition = (keyName, schema, scan) => {
  const f = operator => function () {
    const copy = [].slice.call(arguments);
    const existingValueKeys = _.keys(scan.request.ExpressionAttributeValues);
    const args = [keyName, operator, existingValueKeys].concat(copy);
    const cond = expressions.buildFilterExpression.apply(null, args);
    return scan.addFilterCondition(cond);
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
    notNull: f('attribute_exists'),
    contains: f('contains'),
    notContains: f('NOT contains'),
    in: f('IN'),
    beginsWith: f('begins_with'),
    between: f('BETWEEN')
  };
};

const Scan = module.exports = function (table, serializer) {
  Op.call(this, table, serializer);
  this.options = { loadAll: false };

  this.request = {};
};

inherits(Scan, Op);

Scan.prototype.segments = function (segment, totalSegments) {
  this.request.Segment = segment;
  this.request.TotalSegments = totalSegments;

  return this;
};

Scan.prototype.where = function (keyName) {
  return internals.keyCondition(keyName, this.table.schema, this);
};

Scan.prototype.exec = co(function* () {
  const runScan = (params) => this.table.runScan(params);
  return utils.paginatedRequest(this, runScan);
});
