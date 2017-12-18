'use strict';

const co = require('co').wrap
const util = require('util');
const _ = require('lodash');
const events = require('events');

const Item = module.exports = function (attrs, table) {
  events.EventEmitter.call(this);

  this.table = table;

  this.set(attrs || {});
};

util.inherits(Item, events.EventEmitter);

Item.prototype.get = function (key) {
  if (key) {
    return this.attrs[key];
  } else {
    return this.attrs;
  }
};

Item.prototype.set = function (params) {
  this.attrs = _.merge({}, this.attrs, params);

  return this;
};

Item.prototype.save = co(function* () {
  const item = yield this.table.create(this.attrs)
  this.set(item.attrs)
  return item
});

Item.prototype.update = co(function* (options) {
  const item = yield this.table.update(this.attrs, options)
  if (item) {
    this.set(item.attrs);
  }

  return item
});

Item.prototype.destroy = co(function* (options) {
  yield this.table.destroy(this.attrs, options);
});

Item.prototype.toJSON = function () {
  return _.cloneDeep(this.attrs);
};

Item.prototype.toPlainObject = function () {
  return _.cloneDeep(this.attrs);
};
