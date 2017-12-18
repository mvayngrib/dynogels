'use strict';

const co = require('co').wrap
const dynogels = require('../../index');
const chai = require('chai');
const expect = chai.expect;
const _ = require('lodash');
const Joi = require('joi');
const helper = require('../test-helper');

chai.should();

describe('Create Tables Integration Tests', function () {
  this.timeout(0);

  before(() => {
    dynogels.dynamoDriver(helper.realDynamoDB());
  });

  afterEach(() => {
    dynogels.reset();
  });

  it('should create table with hash key', co(function* () {
    const Model = dynogels.define('dynogels-create-table-test', {
      hashKey: 'id',
      tableName: helper.randomName('dynogels-createtable-Accounts'),
      schema: {
        id: Joi.string(),
      }
    });

    const result = yield Model.createTable()
    const desc = result.TableDescription;
    expect(desc).to.exist;
    expect(desc.KeySchema).to.eql([{ AttributeName: 'id', KeyType: 'HASH' }]);

    expect(desc.AttributeDefinitions).to.eql([
      { AttributeName: 'id', AttributeType: 'S' },
    ]);

    yield Model.deleteTable();
  }));

  it('should create table with hash and range key', co(function* () {
    const Model = dynogels.define('dynogels-createtable-rangekey', {
      hashKey: 'name',
      rangeKey: 'age',
      tableName: helper.randomName('dynogels-createtable-rangekey'),
      schema: {
        name: Joi.string(),
        age: Joi.number(),
      }
    });

    const result = yield Model.createTable()
    const desc = result.TableDescription;

    expect(desc).to.exist;

    expect(desc.AttributeDefinitions).to.eql([
      { AttributeName: 'name', AttributeType: 'S' },
      { AttributeName: 'age', AttributeType: 'N' }
    ]);

    expect(desc.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'age', KeyType: 'RANGE' }
    ]);

    yield Model.deleteTable();
  }));

  it('should create table with local secondary index', co(function* () {
    const Model = dynogels.define('dynogels-createtable-rangekey', {
      hashKey: 'name',
      rangeKey: 'age',
      tableName: helper.randomName('dynogels-createtable-local-idx'),
      schema: {
        name: Joi.string(),
        age: Joi.number(),
        nick: Joi.string(),
        time: Joi.date()
      },
      indexes: [
        { hashKey: 'name', rangeKey: 'nick', type: 'local', name: 'NickIndex' },
        { hashKey: 'name', rangeKey: 'time', type: 'local', name: 'TimeIndex' },
      ]
    });

    const result = yield Model.createTable()
    const desc = result.TableDescription;

    expect(desc).to.exist;

    expect(desc.AttributeDefinitions).to.eql([
      { AttributeName: 'name', AttributeType: 'S' },
      { AttributeName: 'age', AttributeType: 'N' },
      { AttributeName: 'nick', AttributeType: 'S' },
      { AttributeName: 'time', AttributeType: 'S' }
    ]);

    expect(desc.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'age', KeyType: 'RANGE' }
    ]);

    expect(desc.LocalSecondaryIndexes).to.have.length(2);

    const nickIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'NickIndex' });
    expect(nickIndex.IndexName).to.eql('NickIndex');
    expect(nickIndex.Projection).to.eql({ ProjectionType: 'ALL' });
    expect(nickIndex.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'nick', KeyType: 'RANGE' },
    ]);

    const timeIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'TimeIndex' });
    expect(timeIndex.IndexName).to.eql('TimeIndex');
    expect(timeIndex.Projection).to.eql({ ProjectionType: 'ALL' });
    expect(timeIndex.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'time', KeyType: 'RANGE' },
    ]);

    yield Model.deleteTable();
  }));

  it('should create table with local secondary index with custom projection', co(function* () {
    const Model = dynogels.define('dynogels-createtable-local-proj', {
      hashKey: 'name',
      rangeKey: 'age',
      tableName: helper.randomName('dynogels-createtable-local-proj'),
      schema: {
        name: Joi.string(),
        age: Joi.number(),
        nick: Joi.string()
      },
      indexes: [{
        hashKey: 'name',
        rangeKey: 'nick',
        type: 'local',
        name: 'KeysOnlyNickIndex',
        projection: { ProjectionType: 'KEYS_ONLY' }
      }]
    });

    const result = yield Model.createTable()
    const desc = result.TableDescription;

    expect(desc).to.exist;

    expect(desc.AttributeDefinitions).to.eql([
      { AttributeName: 'name', AttributeType: 'S' },
      { AttributeName: 'age', AttributeType: 'N' },
      { AttributeName: 'nick', AttributeType: 'S' }
    ]);

    expect(desc.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'age', KeyType: 'RANGE' }
    ]);

    const nickIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'KeysOnlyNickIndex' });
    expect(nickIndex.IndexName).to.eql('KeysOnlyNickIndex');
    expect(nickIndex.Projection).to.eql({ ProjectionType: 'KEYS_ONLY' });
    expect(nickIndex.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'nick', KeyType: 'RANGE' },
    ]);

    yield Model.deleteTable();
  }));

  it('should create table with global index', co(function* () {
    const Model = dynogels.define('dynogels-createtable-global', {
      hashKey: 'name',
      rangeKey: 'age',
      tableName: helper.randomName('dynogels-createtable-global'),
      schema: {
        name: Joi.string(),
        age: Joi.number(),
        nick: Joi.string()
      },
      indexes: [{ hashKey: 'nick', type: 'global', name: 'GlobalNickIndex' }]
    });

    const result = yield Model.createTable()
    const desc = result.TableDescription;

    expect(desc).to.exist;

    expect(desc.AttributeDefinitions).to.eql([
      { AttributeName: 'name', AttributeType: 'S' },
      { AttributeName: 'age', AttributeType: 'N' },
      { AttributeName: 'nick', AttributeType: 'S' }
    ]);

    expect(desc.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'age', KeyType: 'RANGE' }
    ]);

    const nickIndex = _.find(desc.GlobalSecondaryIndexes, { IndexName: 'GlobalNickIndex' });
    expect(nickIndex.IndexName).to.eql('GlobalNickIndex');
    expect(nickIndex.Projection).to.eql({ ProjectionType: 'ALL' });
    expect(nickIndex.KeySchema).to.eql([
      { AttributeName: 'nick', KeyType: 'HASH' },
    ]);
    expect(nickIndex.ProvisionedThroughput).to.eql({ ReadCapacityUnits: 1, WriteCapacityUnits: 1 });

    yield Model.deleteTable();
  }));

  it('should create table with global index with optional settings', co(function* () {
    const Model = dynogels.define('dynogels-createtable-global', {
      hashKey: 'name',
      rangeKey: 'age',
      tableName: helper.randomName('dynogels-createtable-global'),
      schema: {
        name: Joi.string(),
        age: Joi.number(),
        nick: Joi.string(),
        wins: Joi.number()
      },
      indexes: [{
        hashKey: 'nick',
        type: 'global',
        name: 'GlobalNickIndex',
        projection: { NonKeyAttributes: ['wins'], ProjectionType: 'INCLUDE' },
        readCapacity: 10,
        writeCapacity: 5
      }]
    });

    const result = yield Model.createTable()
    const desc = result.TableDescription;

    expect(desc).to.exist;

    expect(desc.AttributeDefinitions).to.eql([
      { AttributeName: 'name', AttributeType: 'S' },
      { AttributeName: 'age', AttributeType: 'N' },
      { AttributeName: 'nick', AttributeType: 'S' }
    ]);

    expect(desc.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'age', KeyType: 'RANGE' }
    ]);

    const nickIndex = _.find(desc.GlobalSecondaryIndexes, { IndexName: 'GlobalNickIndex' });
    expect(nickIndex.IndexName).to.eql('GlobalNickIndex');
    expect(nickIndex.Projection).to.eql({ ProjectionType: 'INCLUDE', NonKeyAttributes: ['wins'] });
    expect(nickIndex.KeySchema).to.eql([
      { AttributeName: 'nick', KeyType: 'HASH' },
    ]);
    expect(nickIndex.ProvisionedThroughput).to.eql({ ReadCapacityUnits: 10, WriteCapacityUnits: 5 });

    yield Model.deleteTable();
  }));

  it('should create table with global and local indexes', co(function* () {
    const Model = dynogels.define('dynogels-createtable-both-indexes', {
      hashKey: 'name',
      rangeKey: 'age',
      tableName: helper.randomName('dynogels-createtable-both-indexes'),
      schema: {
        name: Joi.string(),
        age: Joi.number(),
        nick: Joi.string(),
        wins: Joi.number()
      },
      indexes: [
        { hashKey: 'name', rangeKey: 'nick', type: 'local', name: 'NameNickIndex' },
        { hashKey: 'name', rangeKey: 'wins', type: 'local', name: 'NameWinsIndex' },
        { hashKey: 'nick', type: 'global', name: 'GlobalNickIndex' },
        { hashKey: 'age', rangeKey: 'wins', type: 'global', name: 'GlobalAgeWinsIndex' }
      ]
    });

    const result = yield Model.createTable()
    const desc = result.TableDescription;

    expect(desc).to.exist;

    expect(desc.AttributeDefinitions).to.eql([
      { AttributeName: 'name', AttributeType: 'S' },
      { AttributeName: 'age', AttributeType: 'N' },
      { AttributeName: 'nick', AttributeType: 'S' },
      { AttributeName: 'wins', AttributeType: 'N' }
    ]);

    expect(desc.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'age', KeyType: 'RANGE' }
    ]);

    expect(desc.GlobalSecondaryIndexes).to.have.length(2);

    const nickIndex = _.find(desc.GlobalSecondaryIndexes, { IndexName: 'GlobalNickIndex' });
    expect(nickIndex.IndexName).to.eql('GlobalNickIndex');
    expect(nickIndex.Projection).to.eql({ ProjectionType: 'ALL' });
    expect(nickIndex.KeySchema).to.eql([
      { AttributeName: 'nick', KeyType: 'HASH' },
    ]);
    expect(nickIndex.ProvisionedThroughput).to.eql({ ReadCapacityUnits: 1, WriteCapacityUnits: 1 });

    const ageWinsIndex = _.find(desc.GlobalSecondaryIndexes, { IndexName: 'GlobalAgeWinsIndex' });
    expect(ageWinsIndex.IndexName).to.eql('GlobalAgeWinsIndex');
    expect(ageWinsIndex.Projection).to.eql({ ProjectionType: 'ALL' });
    expect(ageWinsIndex.KeySchema).to.eql([
      { AttributeName: 'age', KeyType: 'HASH' },
      { AttributeName: 'wins', KeyType: 'RANGE' },
    ]);
    expect(ageWinsIndex.ProvisionedThroughput).to.eql({ ReadCapacityUnits: 1, WriteCapacityUnits: 1 });

    expect(desc.LocalSecondaryIndexes).to.have.length(2);

    const nameNickIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'NameNickIndex' });
    expect(nameNickIndex.IndexName).to.eql('NameNickIndex');
    expect(nameNickIndex.Projection).to.eql({ ProjectionType: 'ALL' });
    expect(nameNickIndex.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'nick', KeyType: 'RANGE' },
    ]);

    const nameWinsIndex = _.find(desc.LocalSecondaryIndexes, { IndexName: 'NameWinsIndex' });
    expect(nameWinsIndex.IndexName).to.eql('NameWinsIndex');
    expect(nameWinsIndex.Projection).to.eql({ ProjectionType: 'ALL' });
    expect(nameWinsIndex.KeySchema).to.eql([
      { AttributeName: 'name', KeyType: 'HASH' },
      { AttributeName: 'wins', KeyType: 'RANGE' },
    ]);

    yield Model.deleteTable();
  }));
});

describe('Update Tables Integration Tests', function () {
  this.timeout(0);
  let Tweet;
  let tableName;

  before(co(function* () {
    dynogels.dynamoDriver(helper.realDynamoDB());

    tableName = helper.randomName('dynogels-updateTable-Tweets');

    Tweet = dynogels.define('dynogels-update-table-test', {
      hashKey: 'UserId',
      rangeKey: 'TweetID',
      tableName: tableName,
      schema: {
        UserId: Joi.string(),
        TweetID: dynogels.types.uuid(),
        content: Joi.string(),
        PublishedDateTime: Joi.date().default(Date.now, 'now')
      }
    });

    yield dynogels.createTables();
  }));

  afterEach(() => {
    dynogels.reset();
  });

  it('should add global secondary index', co(function* () {
    Tweet = dynogels.define('dynogels-update-table-test', {
      hashKey: 'UserId',
      rangeKey: 'TweetID',
      tableName: tableName,
      schema: {
        UserId: Joi.string(),
        TweetID: dynogels.types.uuid(),
        content: Joi.string(),
        PublishedDateTime: Joi.date().default(Date.now, 'now')
      },
      indexes: [
        { hashKey: 'UserId', rangeKey: 'PublishedDateTime', type: 'global', name: 'PublishedDateTimeIndex' }
      ]
    });

    yield Tweet.updateTable()
    const data = yield Tweet.describeTable()
    const globalIndexes = _.get(data, 'Table.GlobalSecondaryIndexes');
    expect(globalIndexes).to.have.length(1);

    const idx = _.first(globalIndexes);
    expect(idx.IndexName).to.eql('PublishedDateTimeIndex');
    expect(idx.KeySchema).to.eql([{ AttributeName: 'UserId', KeyType: 'HASH' },
                                  { AttributeName: 'PublishedDateTime', KeyType: 'RANGE' }]);
    expect(idx.Projection).to.eql({ ProjectionType: 'ALL' });
  }));
});
