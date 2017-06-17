// based on /parse-server/blob/master/spec/Mongo*.spec.js
/// <reference path="../node_modules/mocha-typescript/globals.d.ts" />
import { suite, test, slow, timeout } from 'mocha-typescript';
import { should, expect, assert } from 'chai';
import { DynamoDB as DAdapter } from '../src/';
import { Partition } from '../src/Partition';
import { SchemaPartition as Schema, mongoSchemaToParseSchema } from '../src/SchemaPartition';
import { Adapter } from '../src/Adapter';

const DDB = new DAdapter('parse-server', { 
    endpoint : 'http://localhost:8000',
    region : 'earth',
    accessKeyId : 'key',
    secretAccessKey : 'secret'
});

const $ = DDB.getAdapter();

@suite class DDBSchemaParititon {
    
}