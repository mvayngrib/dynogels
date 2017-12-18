'use strict';

const Promise = require('bluebird')
const co = require('co').wrap
const _ = require('lodash');

const internals = {};

internals.buildInitialGetItemsRequest = (tableName, keys, options) => {
  const request = {};

  request[tableName] = _.merge({}, { Keys: keys }, options);

  return { RequestItems: request };
};

internals.serializeKeys = (keys, table, serializer) => keys.map(key => serializer.buildKey(key, null, table.schema));

internals.mergeResponses = (tableName, responses) => {
  const base = {
    Responses: {},
    ConsumedCapacity: []
  };

  base.Responses[tableName] = [];

  return responses.reduce((memo, resp) => {
    if (resp.Responses && resp.Responses[tableName]) {
      memo.Responses[tableName] = memo.Responses[tableName].concat(resp.Responses[tableName]);
    }

    return memo;
  }, base);
};

internals.paginatedRequest = co(function* (request, table) {
  const moreKeysToProcessFunc = () => request !== null && !_.isEmpty(request)
  const responses = [];
  let response
  while (moreKeysToProcessFunc()) {
    try {
      response = yield table.runBatchGetItems(request)
    } catch (err) {
      if (!err.retryable) throw err

      continue
    }

    request = response.UnprocessedKeys;
    if (moreKeysToProcessFunc()) {
      request = { RequestItems: request };
    }

    responses.push(response);
  }

  return internals.mergeResponses(table.tableName(), responses)
});

internals.buckets = keys => {
  const buckets = [];

  while (keys.length) {
    buckets.push(keys.splice(0, 100));
  }

  return buckets;
};

internals.initialBatchGetItems = co(function* (keys, table, serializer, options) {
  const serializedKeys = internals.serializeKeys(keys, table, serializer);
  const request = internals.buildInitialGetItemsRequest(table.tableName(), serializedKeys, options);

  const data = yield internals.paginatedRequest(request, table)
  const dynamoItems = data.Responses[table.tableName()];
  return _.map(dynamoItems, i => table.initItem(serializer.deserializeItem(i)));
});

internals.getItems = (table, serializer) => co(function* (keys, options={}) {
  const results = yield Promise.map(
    internals.buckets(_.clone(keys)),
    key => internals.initialBatchGetItems(key, table, serializer, options)
  );

  return _.flatten(results)
});

module.exports = (table, serializer) => ({
  getItems: internals.getItems(table, serializer)
});
