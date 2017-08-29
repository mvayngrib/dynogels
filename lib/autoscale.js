'use strict';

const _ = require('lodash');
const async = require('async');

function getCommonParams(scaler) {
  const { table, indexName, role } = scaler;
  let ResourceId = `table/${table.tableName()}`;
  if (indexName) ResourceId += `/index/${indexName}`;

  return {
    ResourceId,
    RoleARN: role,
    ServiceNamespace: 'dynamodb'
  };
}

function scale(scaler, callback) {
  const tasks = [];
  const common = getCommonParams(scaler);
  const { indexName, readCapacity, writeCapacity, autoscaling } = scaler;
  const resourceType = indexName ? 'index' : 'table';
  if (readCapacity) {
    tasks.push(_.extend({
      MaxCapacity: readCapacity.max,
      MinCapacity: readCapacity.min,
      ScalableDimension: `dynamodb:${resourceType}:ReadCapacityUnits`
    }, common));
  }

  if (writeCapacity) {
    tasks.push(_.extend({
      MaxCapacity: writeCapacity.max,
      MinCapacity: writeCapacity.min,
      ScalableDimension: `dynamodb:${resourceType}:WriteCapacityUnits`
    }, common));
  }

  const execScalingOp = autoscaling.registerScalableTarget.bind(autoscaling);
  return async.map(tasks, execScalingOp, callback);
}

function Scaler(options) {
  if (!(this instanceof Scaler)) {
    return Scaler(options);
  }

  this.options = _.clone(options);
}

Scaler.prototype.autoscaling = function (autoscaling) {
  this.options.autoscaling = autoscaling;
  return this;
};

Scaler.prototype.table = function (table) {
  this.options.table = table;
  return this;
};

Scaler.prototype.indexName = function (indexName) {
  this.options.indexName = indexName;
  return this;
};

Scaler.prototype.readCapacity = function ({ min, max }) {
  this.options.readCapacity = { min, max };
  return this;
};

Scaler.prototype.writeCapacity = function ({ min, max }) {
  this.options.writeCapacity = { min, max };
  return this;
};

Scaler.prototype.role = function (arn) {
  this.options.role = arn;
  return this;
};

Scaler.prototype.exec = function (callback) {
  return scale(this.options, callback);
};

exports = module.exports = Scaler;
exports.autoscale = (options, callback) => new Scaler(options).exec(callback);
