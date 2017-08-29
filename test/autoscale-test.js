'use strict';

const helper = require('./test-helper');
const Schema = require('../lib/schema');
const Autoscale = require('../lib/autoscale');
const chai = require('chai');
const sinon = require('sinon');
const expect = chai.expect;
const Joi = require('joi');

chai.should();

describe('Autoscale', () => {
  let table;
  let autoscaling;
  const tableName = 'mytable';
  const RoleARN = 'myrole';
  const config = {
    tableName,
    hashKey: 'name',
    rangeKey: 'email',
    schema: {
      name: Joi.string(),
      email: Joi.string()
    }
  };

  beforeEach(() => {
    table = helper.mockTable();
    table.config = { name: 'accounts' };
    table.schema = new Schema(config);
    table.tableName = () => tableName;
    autoscaling = {
      registerScalableTarget: (params, callback) => {
        callback();
      }
    };
  });

  describe('#exec', () => {
    it('should autoscale table', done => {
      const regSpy = sinon.spy(autoscaling, 'registerScalableTarget');
      const ResourceId = `table/${tableName}`;
      new Autoscale({
        autoscaling,
        table,
        role: RoleARN,
        readCapacity: { min: 1, max: 5 },
        writeCapacity: { min: 2, max: 6 }
      })
      .exec(err => {
        expect(err).to.not.exist;
        expect(regSpy.callCount).to.equal(2);
        expect(regSpy.getCall(0).args[0]).to.deep.equal({
          MinCapacity: 1,
          MaxCapacity: 5,
          ResourceId,
          RoleARN,
          ScalableDimension: 'dynamodb:table:ReadCapacityUnits',
          ServiceNamespace: 'dynamodb'
        });

        expect(regSpy.getCall(1).args[0]).to.deep.equal({
          MinCapacity: 2,
          MaxCapacity: 6,
          ResourceId,
          RoleARN,
          ScalableDimension: 'dynamodb:table:WriteCapacityUnits',
          ServiceNamespace: 'dynamodb'
        });

        done();
      });
    });

    it('should autoscale index', done => {
      const regSpy = sinon.spy(autoscaling, 'registerScalableTarget');
      const indexName = 'myindex';
      const ResourceId = `table/${tableName}/index/${indexName}`;
      new Autoscale({
        autoscaling,
        table,
        role: RoleARN,
        indexName,
        readCapacity: { min: 1, max: 5 },
        writeCapacity: { min: 2, max: 6 }
      })
      .exec(err => {
        expect(err).to.not.exist;
        expect(regSpy.callCount).to.equal(2);
        expect(regSpy.getCall(0).args[0]).to.deep.equal({
          MinCapacity: 1,
          MaxCapacity: 5,
          ResourceId,
          RoleARN,
          ScalableDimension: 'dynamodb:index:ReadCapacityUnits',
          ServiceNamespace: 'dynamodb'
        });

        expect(regSpy.getCall(1).args[0]).to.deep.equal({
          MinCapacity: 2,
          MaxCapacity: 6,
          ResourceId,
          RoleARN,
          ScalableDimension: 'dynamodb:index:WriteCapacityUnits',
          ServiceNamespace: 'dynamodb'
        });

        done();
      });
    });
  });
});
