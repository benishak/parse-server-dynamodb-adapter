/// <reference path="../node_modules/mocha-typescript/globals.d.ts" />
import { suite, context, test, slow, timeout } from 'mocha-typescript';
import { should, expect, assert } from 'chai';
import { DynamoDB as DAdapter } from '../src/';
import { Partition  } from '../src/DynamoPartition';
import { Adapter } from '../src/DynamoAdapter';
import { DynamoDB } from 'aws-sdk';
import { mongoSchemaToParseSchema } from '../src/SchemaPartition';
import { generate as randomString } from 'randomstring';
import * as Promise from 'bluebird';

const AWS = require('aws-sdk-mock');

const database = 'parse';
const settings = { 
    region : 'eu-central-1',
    accessKeyId: 'key',
    secretAccessKey: 'secret'
}

const DDB = new DAdapter('parse-server', { 
    endpoint : 'http://localhost:8000',
    region : 'earth',
    accessKeyId : 'key',
    secretAccessKey : 'secret',
    apiVersion: '2012-08-10'
});

const $ = DDB.getAdapter();

@suite class DDBAdapter {

    @test 'DynamoDB Adapter : getAdapter'() {
        let dynamo = new DAdapter(database, settings);
        let adapter = dynamo.getAdapter();
        expect(adapter instanceof Adapter).to.be.true;
        expect(adapter.database).to.be.equal(database);
        expect(adapter.settings).to.be.equal(settings);
        expect(adapter.service instanceof DynamoDB).to.be.true;
    }

    @test 'DynamoDB Adapter : Create adaptive Collection'() {
        let dynamo = new DAdapter(database, settings);
        let adapter = dynamo.getAdapter();
        let partition = adapter._adaptiveCollection('test');
        expect(partition instanceof Partition).to.be.true;
        expect(partition.database).to.be.equal(database);
        expect(partition.className).to.be.equal('test');
    }

    @test 'DynamoDB Adapter : Return Schema Collection'() {
        let dynamo = new DAdapter(database, settings);
        let adapter = dynamo.getAdapter();
        let partition = adapter._schemaCollection();
        expect(partition instanceof Partition).to.be.true;
        expect(partition.database).to.be.equal(database);
        expect(partition.className).to.be.equal('_SCHEMA');
    }
}

@suite class DDBSOps {

    before(done) {
        $.deleteAllClasses().then(() => {
            done();
        });
    }

    @test 'stores objectId in _id'(done) {
        $.createObject('Foo', { fields: {} }, { objectId: 'abcde' })
            .then(() => $._rawFind('Foo'))
            .then(results => {
                expect(results.length).to.be.equal(1);
                var obj = results[0];
                expect(obj._id).to.be.equal('abcde');
                expect(obj.objectId).to.be.undefined;
                done();
            });
    }

    @test 'stores pointers with a _p_ prefi'(done) {
        const obj = {
            objectId: 'bar',
            aPointer: {
                __type: 'Pointer',
                className: 'JustThePointer',
                objectId: 'qwerty'
            }
        }

        $.createObject('APointerDarkly', {
            fields: {
                objectId: { type: 'String' },
                aPointer: { type: 'Pointer', targetClass: 'JustThePointer' },
            }
        }, obj)
        .then(() => $._rawFind('APointerDarkly'))
        .then(results => {
            expect(results.length).to.be.equal(1);
            const output = results[0];
            expect(typeof output._id).to.be.equal('string');
            expect(typeof output._p_aPointer).to.be.equal('string');
            expect(output._p_aPointer).to.be.equal('JustThePointer$qwerty');
            expect(output.aPointer).to.be.undefined;
            done();
        });
    }

    @test 'handles object and subdocument'(done) {
        const schema = { objectId: { type : 'String' }, fields : { subdoc: { type: 'Object' } } };
        const objectId = randomString(10);
        const className = randomString(7);

        const obj = { 
            objectId: objectId,
            subdoc: {foo: 'bar', wu: 'tan'}
        };
        $.createObject(className, schema, obj)
        .then(() => $._rawFind(className))
        .then(results => {
            expect(results.length).to.be.equal(1);
            const mob = results[0];
            expect(typeof mob.subdoc).to.be.equal('object');
            expect(mob.subdoc.foo).to.be.equal('bar');
            expect(mob.subdoc.wu).to.be.equal('tan');
            const obj = { 
                subdoc : {
                    foo : 'bar',
                    wu : 'clan'
                }
            }
            return $.findOneAndUpdate(className, schema, { objectId: objectId }, obj);
        })
        .then(() => $._rawFind(className))
        .then(results => {
            expect(results.length).to.be.equal(1);
            const mob = results[0];
            expect(typeof mob.subdoc).to.be.equal('object');
            expect(mob.subdoc.foo).to.be.equal('bar');
            expect(mob.subdoc.wu).to.be.equal('clan');
            done();
        })
        .catch(error => {
            console.log(error);
            expect(error).to.be.undefined;
            done();
        })
    }

    @test 'handles creating an array, object, date, file'(done) {
        const adapter = $;
        const objectId = randomString(10);
        const className = randomString(7);

        const obj = {
            objectId : objectId,
            array: [1, 2, 3],
            object: {foo: 'bar'},
            date: {
                __type: 'Date',
                iso: '2016-05-26T20:55:01.154Z',
            },
            file: {
                __type: 'File',
                name: '7aefd44420d719adf65d16d52e688baf_license.txt',
                url: 'http://localhost:1337/files/app/7aefd44420d719adf65d16d52e688baf_license.txt' 
            }
        };
        const schema = {
            fields: {
                array: { type: 'Array' },
                object: { type: 'Object' },
                date: { type: 'Date' },
                file: { type: 'File' }
            }
        };
        adapter.createObject(className, schema, obj)
        .then(() => adapter._rawFind(className, {}))
        .then(results => {
            expect(results.length).to.be.equal(1);
            const mob = results[0];
            expect(mob.array instanceof Array).to.be.equal(true);
            expect(typeof mob.object).to.be.equal('object');
            expect(typeof mob.date).to.be.equal('string');
            return adapter.find(className, schema, {}, {});
        })
        .then(results => {
            expect(results.length).to.be.equal(1);
            const mob = results[0];
            expect(mob.objectId).to.be.equal(objectId);
            expect(mob.array instanceof Array).to.be.equal(true);
            expect(typeof mob.object).to.be.equal('object');
            expect(mob.date).to.be.equal('2016-05-26T20:55:01.154Z');
            expect(mob.file.__type).to.be.equal('File');
            done();
        })
        .catch(error => {
            console.log(error);
            expect(error).to.be.undefined;
            done();
        });
    }

    @test 'handles updating a single object with array, object, date'(done) {
        const adapter = $;
        const objectId = randomString(10);
        const className = randomString(7);

        const schema = {
            fields: {
                array: { type: 'Array' },
                object: { type: 'Object' },
                date: { type: 'Date' },
            }
        };


        adapter.createObject(className, schema, { objectId: objectId })
        .then(() => adapter._rawFind(className))
        .then(results => {
            expect(results.length).to.be.equal(1);
            const update = {
                array: [1, 2, 3],
                object: {foo: 'bar'},
                date: {
                    __type: 'Date',
                    iso: '2016-05-26T20:55:01.154Z',
                },
            };
            const query = { objectId: objectId };
            return adapter.findOneAndUpdate(className, schema, query, update)
        })
        .then(results => {
            const mob = results;
            expect(mob.array instanceof Array).to.be.equal(true);
            expect(typeof mob.object).to.be.equal('object');
            expect(typeof mob.date).to.be.equal('string');
            expect(mob.date).to.be.equal('2016-05-26T20:55:01.154Z');
            return adapter._rawFind(className);
        })
        .then(results => {
            expect(results.length).to.be.equal(1);
            const mob = results[0];
            expect(mob.array instanceof Array).to.be.equal(true);
            expect(typeof mob.object).to.be.equal('object');
            expect(typeof mob.date).to.be.equal('string');
            expect(mob.date).to.be.equal('2016-05-26T20:55:01.154Z');
            done();
        })
        .catch(error => {
            console.log(error);
            expect(error).to.be.undefined;
            done();
        });
    }
}