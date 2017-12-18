'use strict';

const co = require('co').wrap
const helper = require('./test-helper');
const _ = require('lodash');
const Joi = require('joi');
const Table = require('../lib/table');
const Schema = require('../lib/schema');
const Query = require('../lib//query');
const Scan = require('../lib//scan');
const Item = require('../lib/item');
const realSerializer = require('../lib/serializer');
const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
const { promiser } = helper

chai.should();

describe('table', () => {
  let table;
  let serializer;
  let docClient;
  let dynamodb;
  let logger;

  beforeEach(() => {
    serializer = helper.mockSerializer();
    docClient = helper.mockDocClient();
    dynamodb = docClient.service;
    logger = helper.testLogger();
  });

  describe('#get', () => {
    it('should get item by hash key', co(function* () {
      const config = {
        hashKey: 'email'
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' }
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'test dude' }
      };

      docClient.get.withArgs(request).returns(promiser(null, resp));
      const account = yield table.get('test@test.com')
      account.should.be.instanceof(Item);
      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('test dude');
    }));

    it('should get item by hash and range key', co(function* () {
      const config = {
        hashKey: 'name',
        rangeKey: 'email'
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          name: 'Tim Tester',
          email: 'test@test.com'
        }
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'Tim Tester' }
      };

      docClient.get.withArgs(request).returns(promiser(null, resp));

      const account = yield table.get('Tim Tester', 'test@test.com')
      account.should.be.instanceof(Item);
      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('Tim Tester');
    }));

    it('should get item by hash key and options', co(function* () {
      const config = {
        hashKey: 'email',
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ConsistentRead: true
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'test dude' }
      };

      docClient.get.withArgs(request).returns(promiser(null, resp));

      const account = yield table.get('test@test.com', { ConsistentRead: true })
      account.should.be.instanceof(Item);
      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('test dude');

    }));

    it('should get item by hashkey, range key and options', co(function* () {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          name: 'Tim Tester',
          email: 'test@test.com'
        },
        ConsistentRead: true
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'Tim Tester' }
      };

      docClient.get.withArgs(request).returns(promiser(null, resp));

      const account = yield table.get('Tim Tester', 'test@test.com', { ConsistentRead: true })
      account.should.be.instanceof(Item);
      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('Tim Tester');

    }));

    it('should get item from dynamic table by hash key', co(function* () {
      const config = {
        hashKey: 'email',
        tableName: function () {
          return 'accounts_2014';
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts_2014',
        Key: { email: 'test@test.com' }
      };

      const resp = {
        Item: { email: 'test@test.com', name: 'test dude' }
      };

      docClient.get.withArgs(request).returns(promiser(null, resp));

      const account = yield table.get('test@test.com')
      account.should.be.instanceof(Item);
      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('test dude');
    }));

    it('should return error', co(function* () {
      const config = {
        hashKey: 'email',
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      docClient.get.returns(promiser(new Error('Fail')));

      let account
      let err
      try {
        account = yield table.get('test@test.com')
      } catch (e) {
        err = e
      }

      expect(err).to.exist;
      expect(account).to.not.exist;
    }));
  });

  describe('#create', () => {
    it('should create valid item', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Tim Test',
          age: 23
        }
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create(request.Item)
      account.should.be.instanceof(Item);
      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('Tim Test');
    }));

    it('should call apply defaults', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string().default('Foo'),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Foo',
          age: 23
        }
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create({ email: 'test@test.com', age: 23 })
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('Foo');

    }));

    it('should omit null values', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number().allow(null),
          favoriteNumbers: Schema.types.numberSet().allow(null),
          luckyNumbers: Schema.types.numberSet().allow(null)
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const numberSet = sinon.match(value => {
        const s = docClient.createSet([1, 2, 3]);

        value.type.should.eql('Number');
        value.values.should.eql(s.values);

        return true;
      }, 'NumberSet');

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Tim Test',
          luckyNumbers: numberSet
        }
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const item = { email: 'test@test.com', name: 'Tim Test', age: null, favoriteNumbers: [], luckyNumbers: [1, 2, 3] };
      const account = yield table.create(item)
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('Tim Test');
      account.get('luckyNumbers').should.eql([1, 2, 3]);

      expect(account.toJSON()).to.have.keys(['email', 'name', 'luckyNumbers']);
    }));

    it('should omit empty values', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string().allow(''),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          age: 2
        }
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create({ email: 'test@test.com', name: '', age: 2 })
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
      account.get('age').should.equal(2);

    }));

    it('should create item with createdAt timestamp', co(function* () {
      const config = {
        hashKey: 'email',
        timestamps: true,
        schema: {
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          createdAt: sinon.match.string
        }
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create({ email: 'test@test.com' })
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
      account.get('createdAt').should.exist;
    }));

    it('should create item with custom createdAt attribute name', co(function* () {
      const config = {
        hashKey: 'email',
        timestamps: true,
        createdAt: 'created',
        schema: {
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          created: sinon.match.string
        }
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create({ email: 'test@test.com' })
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
      account.get('created').should.exist;
    }));


    it('should create item without createdAt param', co(function* () {
      const config = {
        hashKey: 'email',
        timestamps: true,
        createdAt: false,
        schema: {
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com'
        }
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create({ email: 'test@test.com' })
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
      expect(account.get('createdAt')).to.not.exist;
    }));

    it('should create item with expected option', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
        },
        ExpressionAttributeNames: { '#name': 'name' },
        ExpressionAttributeValues: { ':name': 'Foo Bar' },
        ConditionExpression: '(#name = :name)'
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create({ email: 'test@test.com' }, { expected: { name: 'Foo Bar' } })
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
    }));

    it('should create item with no callback', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
        }
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      yield table.create({ email: 'test@test.com' });

      docClient.put.calledWith(request);
    }));

    it('should return validation error', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      try {
        yield table.create({ email: 'test@test.com', name: [1, 2, 3] })
        throw new Error('should have failed to create')
      } catch (err) {
        expect(err).to.match(/ValidationError/);
        sinon.assert.notCalled(docClient.put);
      }

    }));

    it('should create item with condition expression on hashkey when overwrite flag is false', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Bob Tester'
        },
        ExpressionAttributeNames: { '#email': 'email' },
        ExpressionAttributeValues: { ':email': 'test@test.com' },
        ConditionExpression: '(#email <> :email)'
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create({ email: 'test@test.com', name: 'Bob Tester' }, { overwrite: false })
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
    }));

    it('should create item with condition expression on hash and range key when overwrite flag is false', co(function* () {
      const config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Bob Tester'
        },
        ExpressionAttributeNames: { '#email': 'email', '#name': 'name' },
        ExpressionAttributeValues: { ':email': 'test@test.com', ':name': 'Bob Tester' },
        ConditionExpression: '(#email <> :email) AND (#name <> :name)'
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create({ email: 'test@test.com', name: 'Bob Tester' }, { overwrite: false })
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
    }));

    it('should create item without condition expression when overwrite flag is true', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Item: {
          email: 'test@test.com',
          name: 'Bob Tester'
        }
      };

      docClient.put.withArgs(request).returns(promiser(null, {}));

      const account = yield table.create({ email: 'test@test.com', name: 'Bob Tester' }, { overwrite: true })
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
    }));
  });

  describe('#update', () => {
    it('should update valid item', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_NEW',
        UpdateExpression: 'SET #name = :name, #age = :age',
        ExpressionAttributeValues: { ':name': 'Tim Test', ':age': 23 },
        ExpressionAttributeNames: { '#name': 'name', '#age': 'age' }
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Tim Test',
        age: 23,
        scores: [97, 86]
      };

      docClient.update.withArgs(request).returns(promiser(null, { Attributes: returnedAttributes }));

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
      const account = yield table.update(item)
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('Tim Test');
      account.get('age').should.equal(23);
      account.get('scores').should.eql([97, 86]);
    }));

    it('should accept falsy key and range values', co(function* () {
      const config = {
        hashKey: 'userId',
        rangeKey: 'timeOffset',
        schema: {
          userId: Joi.number(),
          timeOffset: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('users', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'users',
        Key: { userId: 0, timeOffset: 0 },
        ReturnValues: 'ALL_NEW'
      };

      const returnedAttributes = { userId: 0, timeOffset: 0 };

      docClient.update.withArgs(request).returns(promiser(null, { Attributes: returnedAttributes }));

      const item = { userId: 0, timeOffset: 0 };
      const user = yield table.update(item)
      user.should.be.instanceof(Item);

      user.get('userId').should.equal(0);
      user.get('timeOffset').should.equal(0);

    }));

    it('should update with passed in options', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_OLD',
        UpdateExpression: 'SET #name = :name, #age = :age',
        ExpressionAttributeValues: { ':name_2': 'Foo Bar', ':name': 'Tim Test', ':age': 23 },
        ExpressionAttributeNames: { '#name': 'name', '#age': 'age' },
        ConditionExpression: '(#name = :name_2)'
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Tim Test',
        age: 23,
        scores: [97, 86]
      };

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };

      docClient.update.withArgs(request).returns(promiser(null, { Attributes: returnedAttributes }));

      const getOptions = function () {
        return { ReturnValues: 'ALL_OLD', expected: { name: 'Foo Bar' } };
      };

      const passedOptions = getOptions();

      const account = yield table.update(item, passedOptions)
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('Tim Test');
      account.get('age').should.equal(23);
      account.get('scores').should.eql([97, 86]);

      expect(passedOptions).to.deep.equal(getOptions());

    }));

    it('should update merge update expressions when passed in as options', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_NEW',
        UpdateExpression: 'SET #name = :name, #age = :age ADD #color :c',
        ExpressionAttributeValues: { ':name': 'Tim Test', ':age': 23, ':c': 'red' },
        ExpressionAttributeNames: { '#name': 'name', '#age': 'age', '#color': 'color' }
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Tim Test',
        age: 23,
        scores: [97, 86],
        color: 'red'
      };

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };

      docClient.update.withArgs(request).returns(promiser(null, { Attributes: returnedAttributes }));

      const options = {
        UpdateExpression: 'ADD #color :c',
        ExpressionAttributeValues: { ':c': 'red' },
        ExpressionAttributeNames: { '#color': 'color' }
      };

      const account = yield table.update(item, options)
      account.should.be.instanceof(Item);

      account.get('email').should.equal('test@test.com');
      account.get('name').should.equal('Tim Test');
      account.get('age').should.equal(23);
      account.get('scores').should.eql([97, 86]);
      account.get('color').should.eql('red');

    }));

    it('should update valid item without a callback', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_NEW',
        UpdateExpression: 'SET #name = :name, #age = :age',
        ExpressionAttributeValues: { ':name': 'Tim Test', ':age': 23 },
        ExpressionAttributeNames: { '#name': 'name', '#age': 'age' }
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Tim Test',
        age: 23,
        scores: [97, 86]
      };

      docClient.update.withArgs(request).returns(promiser(null, { Attributes: returnedAttributes }));

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
      table.update(item);

      docClient.update.calledWith(request);
    }));

    it('should return error', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      docClient.update.returns(promiser(new Error('Fail')));

      const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };

      let account
      let err
      try {
        account = yield table.update(item)
      } catch (e) {
        err = e
      }

      expect(err).to.exist;
      expect(account).to.not.exist;
    }));

    it('should handle errors regarding invalid expressions', co(function* () {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
          birthday: Joi.date().iso()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const item = { name: 'Dr. Who', birthday: undefined };

      let account
      let err
      try {
        yield table.update(item)
      } catch (e) {
        err = e
      }

      expect(err).to.exist;
      expect(account).to.not.exist;
    }));
  });

  describe('#query', () => {
    it('should return query object', () => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.query('Bob').should.be.instanceof(Query);
    });
  });

  describe('#scan', () => {
    it('should return scan object', () => {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.scan().should.be.instanceof(Scan);
    });
  });

  describe('#destroy', () => {
    it('should destroy valid item', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com'
        }
      };

      docClient.delete.returns(promiser(null, {}));

      serializer.buildKey.returns(request.Key);

      yield table.destroy('test@test.com')

      serializer.buildKey.calledWith('test@test.com', undefined, s).should.be.true;
      docClient.delete.calledWith(request).should.be.true;

    }));

    it('should destroy valid item with falsy hash and range keys', co(function* () {
      const config = {
        hashKey: 'userId',
        rangeKey: 'timeOffset',
        schema: {
          hashKey: Joi.number(),
          rangeKey: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('users', s, serializer, docClient, logger);

      const request = {
        TableName: 'users',
        Key: {
          userId: 0,
          timeOffset: 0
        }
      };

      docClient.delete.returns(promiser(null, {}));

      serializer.buildKey.returns(request.Key);

      yield table.destroy({ userId: 0, timeOffset: 0 })
      serializer.buildKey.calledWith(0, 0, s).should.be.true;
      docClient.delete.calledWith(request).should.be.true;

    }));

    it('should take optional params', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: { S: 'test@test.com' }
        },
        ReturnValues: 'ALL_OLD'
      };

      docClient.delete.returns(promiser(null, {}));

      serializer.buildKey.returns(request.Key);

      yield table.destroy('test@test.com', { ReturnValues: 'ALL_OLD' })
      serializer.buildKey.calledWith('test@test.com', undefined, s).should.be.true;
      docClient.delete.calledWith(request).should.be.true;

    }));

    it('should parse and return attributes', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: { email: 'test@test.com' },
        ReturnValues: 'ALL_OLD'
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Foo Bar'
      };

      docClient.delete.returns(promiser(null, { Attributes: returnedAttributes }));

      serializer.buildKey.returns(request.Key);
      serializer.deserializeItem.withArgs(returnedAttributes).returns(
        { email: 'test@test.com', name: 'Foo Bar'
      });

      const item = yield table.destroy('test@test.com', { ReturnValues: 'ALL_OLD' })
      serializer.buildKey.calledWith('test@test.com', undefined, s).should.be.true;
      docClient.delete.calledWith(request).should.be.true;

      item.get('name').should.equal('Foo Bar');

    }));

    it('should accept hash and range key', co(function* () {
      const config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com',
          name: 'Foo Bar'
        }
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Foo Bar'
      };

      docClient.delete.returns(promiser(null, { Attributes: returnedAttributes }));

      serializer.buildKey.returns(request.Key);
      serializer.deserializeItem.withArgs(returnedAttributes).returns(
        { email: 'test@test.com', name: 'Foo Bar'
      });

      const item = yield table.destroy('test@test.com', 'Foo Bar')
      serializer.buildKey.calledWith('test@test.com', 'Foo Bar', s).should.be.true;
      docClient.delete.calledWith(request).should.be.true;

      item.get('name').should.equal('Foo Bar');

    }));

    it('should accept hashkey rangekey and options', co(function* () {
      const config = {
        hashKey: 'email',
        rangeKey: 'name',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com',
          name: 'Foo Bar'
        },
        ReturnValues: 'ALL_OLD'
      };

      const returnedAttributes = {
        email: 'test@test.com',
        name: 'Foo Bar'
      };

      docClient.delete.returns(promiser(null, { Attributes: returnedAttributes }));

      serializer.buildKey.returns(request.Key);
      serializer.deserializeItem.withArgs(returnedAttributes).returns(
        { email: 'test@test.com', name: 'Foo Bar'
      });

      const item = yield table.destroy('test@test.com', 'Foo Bar', { ReturnValues: 'ALL_OLD' })
      serializer.buildKey.calledWith('test@test.com', 'Foo Bar', s).should.be.true;
      docClient.delete.calledWith(request).should.be.true;

      item.get('name').should.equal('Foo Bar');

    }));

    it('should serialize expected option', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com'
        },
        ExpressionAttributeNames: { '#name': 'name' },
        ExpressionAttributeValues: { ':name': 'Foo Bar' },
        ConditionExpression: '(#name = :name)'
      };

      docClient.delete.returns(promiser(null, {}));

      serializer.serializeItem.withArgs(s, { name: 'Foo Bar' }, { expected: true }).returns(request.Expected);
      serializer.buildKey.returns(request.Key);

      yield table.destroy('test@test.com', { expected: { name: 'Foo Bar' } })
      serializer.buildKey.calledWith('test@test.com', undefined, s).should.be.true;
      docClient.delete.calledWith(request).should.be.true;

    }));

    it('should call delete item without callback', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com'
        }
      };

      docClient.delete.returns(promiser(null, {}));
      table.destroy('test@test.com');

      docClient.delete.calledWith(request);
    }));

    it('should call delete item with hash key, options and no callback', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, realSerializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        Key: {
          email: 'test@test.com'
        },
        Expected: {
          name: { Value: 'Foo Bar' }
        }
      };

      docClient.delete.returns(promiser(null, {}));
      table.destroy('test@test.com', { expected: { name: 'Foo Bar' } });

      docClient.delete.calledWith(request);

    }));
  });

  describe('#createTable', () => {
    it('should create table with hash key', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        AttributeDefinitions: [
          { AttributeName: 'email', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'email', KeyType: 'HASH' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.returns(promiser(null, {}));

      yield table.createTable({ readCapacity: 5, writeCapacity: 5 })
      dynamodb.createTable.calledWith(request).should.be.true;
    }));

    it('should create table with range key', co(function* () {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        AttributeDefinitions: [
          { AttributeName: 'name', AttributeType: 'S' },
          { AttributeName: 'email', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' },
          { AttributeName: 'email', KeyType: 'RANGE' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.returns(promiser(null, {}));

      yield table.createTable({ readCapacity: 5, writeCapacity: 5 })
      dynamodb.createTable.calledWith(request).should.be.true;
    }));

    it('should create table with stream specification', co(function* () {
      const config = {
        hashKey: 'name',
        schema: {
          name: Joi.string(),
          email: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        AttributeDefinitions: [
          { AttributeName: 'name', AttributeType: 'S' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        StreamSpecification: { StreamEnabled: true, StreamViewType: 'NEW_IMAGE' }
      };

      dynamodb.createTable.returns(promiser(null, {}));

      yield table.createTable({
        readCapacity: 5,
        writeCapacity: 5,
        streamSpecification: {
          streamEnabled: true,
          streamViewType: 'NEW_IMAGE'
        }
      })

      dynamodb.createTable.calledWith(request).should.be.true;
    }));

    it('should create table with secondary index', co(function* () {
      const config = {
        hashKey: 'name',
        rangeKey: 'email',
        indexes: [
          { hashKey: 'name', rangeKey: 'age', name: 'ageIndex', type: 'local' }
        ],
        schema: {
          name: Joi.string(),
          email: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts',
        AttributeDefinitions: [
          { AttributeName: 'name', AttributeType: 'S' },
          { AttributeName: 'email', AttributeType: 'S' },
          { AttributeName: 'age', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'name', KeyType: 'HASH' },
          { AttributeName: 'email', KeyType: 'RANGE' }
        ],
        LocalSecondaryIndexes: [
          {
            IndexName: 'ageIndex',
            KeySchema: [
              { AttributeName: 'name', KeyType: 'HASH' },
              { AttributeName: 'age', KeyType: 'RANGE' }
            ],
            Projection: {
              ProjectionType: 'ALL'
            }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.returns(promiser(null, {}));

      yield table.createTable({ readCapacity: 5, writeCapacity: 5 })
      dynamodb.createTable.calledWith(request).should.be.true;
    }));

    it('should create table with global secondary index', co(function* () {
      const config = {
        hashKey: 'userId',
        rangeKey: 'gameTitle',
        indexes: [
          { hashKey: 'gameTitle', rangeKey: 'topScore', name: 'GameTitleIndex', type: 'global' }
        ],
        schema: {
          userId: Joi.string(),
          gameTitle: Joi.string(),
          topScore: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('gameScores', s, serializer, docClient, logger);

      const request = {
        TableName: 'gameScores',
        AttributeDefinitions: [
          { AttributeName: 'userId', AttributeType: 'S' },
          { AttributeName: 'gameTitle', AttributeType: 'S' },
          { AttributeName: 'topScore', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'userId', KeyType: 'HASH' },
          { AttributeName: 'gameTitle', KeyType: 'RANGE' }
        ],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'GameTitleIndex',
            KeySchema: [
              { AttributeName: 'gameTitle', KeyType: 'HASH' },
              { AttributeName: 'topScore', KeyType: 'RANGE' }
            ],
            Projection: {
              ProjectionType: 'ALL'
            },
            ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.returns(promiser(null, {}));

      yield table.createTable({ readCapacity: 5, writeCapacity: 5 })
      dynamodb.createTable.calledWith(request).should.be.true;
    }));

    it('should create table with global secondary index', co(function* () {
      const config = {
        hashKey: 'userId',
        rangeKey: 'gameTitle',
        indexes: [{
          hashKey: 'gameTitle',
          rangeKey: 'topScore',
          name: 'GameTitleIndex',
          type: 'global',
          readCapacity: 10,
          writeCapacity: 5,
          projection: { NonKeyAttributes: ['wins'], ProjectionType: 'INCLUDE' }
        }],
        schema: {
          userId: Joi.string(),
          gameTitle: Joi.string(),
          topScore: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('gameScores', s, serializer, docClient, logger);

      const request = {
        TableName: 'gameScores',
        AttributeDefinitions: [
          { AttributeName: 'userId', AttributeType: 'S' },
          { AttributeName: 'gameTitle', AttributeType: 'S' },
          { AttributeName: 'topScore', AttributeType: 'N' }
        ],
        KeySchema: [
          { AttributeName: 'userId', KeyType: 'HASH' },
          { AttributeName: 'gameTitle', KeyType: 'RANGE' }
        ],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'GameTitleIndex',
            KeySchema: [
              { AttributeName: 'gameTitle', KeyType: 'HASH' },
              { AttributeName: 'topScore', KeyType: 'RANGE' }
            ],
            Projection: {
              NonKeyAttributes: ['wins'],
              ProjectionType: 'INCLUDE'
            },
            ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 5 }
          }
        ],
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      };

      dynamodb.createTable.returns(promiser(null, {}));

      yield table.createTable({ readCapacity: 5, writeCapacity: 5 })
      dynamodb.createTable.calledWith(request).should.be.true;
    }));
  });

  describe('#describeTable', () => {
    it('should make describe table request', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      const request = {
        TableName: 'accounts'
      };

      dynamodb.describeTable.returns(promiser(null, {}));

      yield table.describeTable()
      dynamodb.describeTable.calledWith(request).should.be.true;
    }));
  });

  describe('#updateTable', () => {
    beforeEach(() => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
    });

    it('should make update table request', co(function* () {
      const request = {
        TableName: 'accounts',
        ProvisionedThroughput: { ReadCapacityUnits: 4, WriteCapacityUnits: 2 }
      };

      dynamodb.describeTable.returns(promiser(null, {}));
      dynamodb.updateTable.returns(promiser(null, {}));

      yield table.updateTable({ readCapacity: 4, writeCapacity: 2 })
      dynamodb.updateTable.calledWith(request).should.be.true;
    }));
  });

  describe('#deleteTable', () => {
    beforeEach(() => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);
    });

    it('should make delete table request', co(function* () {
      const request = {
        TableName: 'accounts'
      };

      dynamodb.deleteTable.returns(promiser(null, {}));

      table.deleteTable(err => {
        expect(err).to.be.null;
        dynamodb.deleteTable.calledWith(request).should.be.true;
      });
    }));
  });

  describe('#tableName', () => {
    it('should return given name', () => {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql('accounts');
    });

    it('should return table name set on schema', () => {
      const config = {
        hashKey: 'email',
        tableName: 'accounts-2014-03',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql('accounts-2014-03');
    });

    it('should return table name returned from function on schema', () => {
      const d = new Date();
      const dateString = [d.getFullYear(), d.getMonth() + 1].join('_');

      const nameFunc = () => `accounts_${dateString}`;

      const config = {
        hashKey: 'email',
        tableName: nameFunc,
        schema: {
          email: Joi.string(),
          name: Joi.string(),
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      table.tableName().should.eql(`accounts_${dateString}`);
    });
  });

  describe('hooks', () => {
    describe('#create', () => {
      it('should call before hooks', co(function* () {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
        docClient.put.returns(promiser(null, {}));

        serializer.serializeItem.withArgs(s, { email: 'test@test.com', name: 'Tommy', age: 23 }).returns({});

        table.before('create', co(function* (data) {
          expect(data).to.exist;
          data.name = 'Tommy';
          return data
        }));

        table.before('create', co(function* (data) {
          expect(data).to.exist;
          data.age = '25';
          return data
        }));

        const created = yield table.create(item)
        created.get('name').should.equal('Tommy');
        created.get('age').should.equal('25');

      }));

      it('should return error when before hook returns error', co(function* () {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        table.before('create', co(function* () {
          throw new Error('fail')
        }));

        let item
        let err
        try {
          yield table.create({ email: 'foo@bar.com' })
        } catch (e) {
          err = e
        }

        expect(err).to.exist;
        expect(item).to.not.exist;
      }));

      it('should call after hook', co(function* () {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
        docClient.put.returns(promiser(null, {}));

        serializer.serializeItem.withArgs(s, item).returns({});

        table.after('create', data => {
          expect(data).to.exist;
        });

        table.create(item);
      }));
    });

    describe('#update', () => {
      it('should call before hook', co(function* () {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
        docClient.update.returns(promiser(null, {}));

        serializer.serializeItem.withArgs(s, item).returns({});

        serializer.buildKey.returns({ email: { S: 'test@test.com' } });
        const modified = { email: 'test@test.com', name: 'Tim Test', age: 44 };
        serializer.serializeItemForUpdate.withArgs(s, 'PUT', modified).returns({});

        serializer.deserializeItem.returns(modified);
        docClient.update.returns(promiser(null, {}));

        let called = false;
        table.before('update', co(function* (data) {
          const attrs = _.merge({}, data, { age: 44 });
          called = true;
          return attrs
        }));

        table.after('update', () => {
          expect(called).to.be.true;
        });

        table.update(item);
      }));

      it('should return error when before hook returns error', co(function* () {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        table.before('update', co(function* () {
          throw new Error('fail')
        }))

        let err
        try {
          yield table.update({})
        } catch (e) {
          err = e
        }

        expect(err).to.exist;
        err.message.should.equal('fail');
      }));

      it('should call after hook', co(function* () {
        const config = {
          hashKey: 'email',
          schema: {
            email: Joi.string(),
            name: Joi.string(),
            age: Joi.number()
          }
        };

        const s = new Schema(config);

        table = new Table('accounts', s, serializer, docClient, logger);

        const item = { email: 'test@test.com', name: 'Tim Test', age: 23 };
        docClient.update.returns(promiser(null, {}));

        serializer.serializeItem.withArgs(s, item).returns({});

        serializer.buildKey.returns({ email: { S: 'test@test.com' } });
        serializer.serializeItemForUpdate.returns({});

        serializer.deserializeItem.returns(item);
        docClient.update.returns(promiser(null, {}));

        return new Promise(resolve => {
          table.after('update', () => resolve());
          table.update(item);
        })
      }));
    });

    it('#destroy should call after hook', co(function* () {
      const config = {
        hashKey: 'email',
        schema: {
          email: Joi.string(),
          name: Joi.string(),
          age: Joi.number()
        }
      };

      const s = new Schema(config);

      table = new Table('accounts', s, serializer, docClient, logger);

      docClient.delete.returns(promiser(null, {}));
      serializer.buildKey.returns({});
      return new Promise(resolve => {
        table.after('destroy', () => resolve());
        table.destroy('test@test.com');
      })
    }));
  });
});
