import { DynamoDB } from 'aws-sdk';
import * as Promise from 'bluebird';
import * as MongoTransform from 'parse-server/lib/Adapters/Storage/Mongo/MongoTransform';
import { Partition } from './DynamoPartition';

const schemaTable = '_SCHEMA';

const DynamoType = type => {
    switch (type.type) {
        case 'String': return 'S';
        case 'Date': return 'M';
        case 'Object': return 'M';
        case 'File': return 'M';
        case 'Boolean': return 'BOOL';
        case 'Pointer': return 'M';
        case 'Relation' : return 'M';
        case 'Number': return 'N';
        case 'GeoPoint': return 'M';
        case 'Array':
            if (type.contents && type.contents.type === 'String') {
                return 'SS';
            } else if (type.contents && type.contents.type === 'Number') {
                return 'NS';
            } else {
                return 'L';
            }0
        default: throw `no type for ${JSON.stringify(type)} yet`;
    }
};

export class Adapter {
    
    database : string;
    settings : Object;

    constructor(database : string, settings : DynamoDB.DocumentClient.DocumentClientOptions) {
        this.database = database,
        this.settings = settings;
    }

    connect() {
        return Promise.resolve();
    }

    _adaptiveCollection(name : string) {
        return new Partition(this.database, name, this.settings);
    }

    _schemaCollection() {
        return this._adaptiveCollection(schemaTable);
    }

    classExists() {
        return Promise.resolve();
    }

    setClassLevelPermissions(className, CLPs) {

    }

    createClass(className, schema) {

    }

    addFieldIfNotExists(className, fieldName, type) {

    }

    deleteClass(className) {

    }

    deleteAllClasses() {

    }

    deleteFields(className, schema, fieldNames) {

    }

    getAllClasses() {

    }

    getClass(className) {

    }

    createObject(className, schema, object) {

    }

    deleteObjectsByQuery(className, schema, query) {

    }

    updateObjectsByQuery(className, schema, query, update) {

    }

    findOneAndUpdate(className, schema, query, update) {

    }

    upsertOneObject(className, schema, query, update) {

    }

    find(className, schema, query, { skip, limit, sort, keys }) {

    }

    ensureUniqueness(className, schema, fieldNames) {

    }

    count(className, schema, query) {
    }

    performInitialization() {
        return Promise.resolve();
    }
}