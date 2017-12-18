'use strict';

const Promise = require('bluebird')
const co = require('co').wrap
const _ = require('lodash');
const wait = millis => new Promise(resolve => setTimeout(resolve, millis))

const internals = {};

internals.createTable = co(function* (model, globalOptions={}, options={}) {
  const tableName = model.tableName();

  let desc
  try {
    desc = yield model.describeTable()
  } catch (err) {}

  if (!desc) {
    model.log.info('creating table: %s', tableName);
    try {
      yield model.createTable(options)
    } catch (err) {
      model.log.warn({ err }, 'failed to create table %s: %s', tableName, err);
      throw err
    }

    model.log.info('waiting for table: %s to become ACTIVE', tableName);
    yield internals.waitTillActive(globalOptions, model);
    return
  }

  try {
    yield model.updateTable()
  } catch (err) {
    model.log.warn({ err }, 'failed to update table %s: %s', tableName, err);
    throw err
  }

  model.log.info('waiting for table: %s to become ACTIVE', tableName);
  yield internals.waitTillActive(globalOptions, model);
});

internals.waitTillActive = co(function* (options, model) {
  let status = 'PENDING';
  let data
  while (status !== 'ACTIVE') {
    if (data) {
      yield wait(options.pollingInterval || 1000)
    }

    data = yield model.describeTable()
    status = data.Table.TableStatus;
  }
});

module.exports = co(function* (models, config) {
  yield Promise.each(_.keys(models), key => internals.createTable(models[key], config.$dynogels, config[key]));
});
