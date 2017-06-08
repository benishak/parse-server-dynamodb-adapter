/// <reference path="../node_modules/mocha-typescript/globals.d.ts" />
import { suite, test, slow, timeout } from 'mocha-typescript';
import { should, expect, assert } from 'chai';
import { DynamoDB as DAdapter } from '../src/';
import { Partition  } from '../src/DynamoPartition';
import { Adapter } from '../src/DynamoAdapter';
import { DynamoDB } from 'aws-sdk';

const AWS = require('aws-sdk-mock');

const database = 'parse';
const settings = { 
    region : 'eu-central-1',
    accessKeyId: 'key',
    secretAccessKey: 'secret'
}

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