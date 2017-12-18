'use strict';

const co = require('co').wrap
const _ = require('lodash');
const Readable = require('stream').Readable;
const AWS = require('aws-sdk');
const wait = millis => new Promise(resolve => setTimeout(resolve, millis))

const utils = module.exports;

utils.waterfall = co(function* (fns) {
  let output = yield fns[0]()
  for (let i = 1; i < fns.length; i++) {
    output = yield fns[i](output)
  }

  return output
})

utils.omitNulls = data => _.omitBy(data, value => _.isNull(value) ||
  _.isUndefined(value) ||
  (_.isArray(value) && _.isEmpty(value)) ||
  (_.isString(value) && _.isEmpty(value)));

utils.mergeResults = (responses, tableName) => {
  const result = {
    Items: [],
    ConsumedCapacity: {
      CapacityUnits: 0,
      TableName: tableName
    },
    Count: 0,
    ScannedCount: 0
  };

  const merged = _.reduce(responses, (memo, resp) => {
    if (!resp) {
      return memo;
    }

    memo.Count += resp.Count || 0;
    memo.ScannedCount += resp.ScannedCount || 0;

    if (resp.ConsumedCapacity) {
      memo.ConsumedCapacity.CapacityUnits += resp.ConsumedCapacity.CapacityUnits || 0;
    }

    if (resp.Items) {
      memo.Items = memo.Items.concat(resp.Items);
    }

    if (resp.LastEvaluatedKey) {
      memo.LastEvaluatedKey = resp.LastEvaluatedKey;
    }

    return memo;
  }, result);

  if (merged.ConsumedCapacity.CapacityUnits === 0) {
    delete merged.ConsumedCapacity;
  }

  if (merged.ScannedCount === 0) {
    delete merged.ScannedCount;
  }

  return merged;
};

utils.paginatedRequest = co(function* (op, runRequestFunc) {
  let lastEvaluatedKey = null;
  const responses = [];
  let retry = true;
  let resp
  while ((op.options.loadAll && lastEvaluatedKey) || retry) {
    try {
      resp = yield runRequestFunc(op.buildRequest())
    } catch (err) {
      if (!err.retryable) throw err

      retry = true;
      continue
    }

    retry = false;
    lastEvaluatedKey = resp.LastEvaluatedKey;
    if (lastEvaluatedKey) {
      op.startKey(lastEvaluatedKey);
    } else {
      op.clearStartKey();
    }

    responses.push(resp)
  }

  return utils.mergeResults(responses, op.table.tableName())
});


utils.streamRequest = (op, runRequestFunc) => {
  let lastEvaluatedKey = null;
  let performRequest = true;

  const stream = new Readable({ objectMode: true });
  const startRead = co(function* () {
    if (!performRequest) {
      return;
    }

    if (lastEvaluatedKey) {
      op.startKey(lastEvaluatedKey);
    }

    let resp
    try {
      resp = yield runRequestFunc(op.buildRequest())
    } catch (err) {
      if (!err.retryable) return stream.emit('error', err)

      yield wait(1000)
      return startRead()
    }

    lastEvaluatedKey = resp.LastEvaluatedKey;

    if (!op.options.loadAll || !lastEvaluatedKey) {
      performRequest = false;
    }

    stream.push(resp);

    if (!op.options.loadAll || !lastEvaluatedKey) {
      stream.push(null);
    }
  });

  stream._read = startRead
  return stream;
};

utils.omitPrimaryKeys = (schema, params) => _.omit(params, schema.hashKey, schema.rangeKey);

utils.strToBin = value => {
  if (typeof(value) !== 'string') {
    const StrConversionError = 'Need to pass in string primitive to be converted to binary.';
    throw new Error(StrConversionError);
  }

  if (AWS.util.isBrowser()) {
    const len = value.length;
    const bin = new Uint8Array(new ArrayBuffer(len));
    for (let i = 0; i < len; i++) {
      bin[i] = value.charCodeAt(i);
    }
    return bin;
  } else {
    return AWS.util.Buffer(value);
  }
};
