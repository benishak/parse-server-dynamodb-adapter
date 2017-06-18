// mocking parse-server/src/Adapters/Storage/Mongo/MongoSchemaCollection.js
import * as Promise from 'bluebird';
import { Partition } from './Partition';
import * as MongoTransform from 'parse-server/lib/Adapters/Storage/Mongo/MongoTransform';

const nonFieldSchemaKeys = ['_id', '_metadata', '_client_permissions'];

const emptyCLPS = Object.freeze({
    find: {},
    get: {},
    create: {},
    update: {},
    delete: {},
    addField: {},
});

const defaultCLPS = Object.freeze({
    find: {'*': true},
    get: {'*': true},
    create: {'*': true},
    update: {'*': true},
    delete: {'*': true},
    addField: {'*': true},
});

type sType = { type, targetClass? }

export function mongoFieldToParseSchemaField(type) {
    if (type[0] === '*') {
        return {
            type: 'Pointer',
            targetClass: type.slice(1),
        };
    }
    if (type.startsWith('relation<')) {
        return {
            type: 'Relation',
            targetClass: type.slice('relation<'.length, type.length - 1),
        };
    }
    switch (type) {
        case 'number':   return {type: 'Number'};
        case 'string':   return {type: 'String'};
        case 'boolean':  return {type: 'Boolean'};
        case 'date':     return {type: 'Date'};
        case 'map':
        case 'object':   return {type: 'Object'};
        case 'array':    return {type: 'Array'};
        case 'geopoint': return {type: 'GeoPoint'};
        case 'file':     return {type: 'File'};
        case 'bytes':    return {type: 'Bytes'};
    }
}

export function mongoSchemaFieldsToParseSchemaFields(schema) {
    var fieldNames = Object.keys(schema).filter(key => nonFieldSchemaKeys.indexOf(key) === -1);
    var response = fieldNames.reduce((obj, fieldName) => {
        obj[fieldName] = mongoFieldToParseSchemaField(schema[fieldName])
        return obj;
    }, {});
    response['ACL'] = {type: 'ACL'};
    response['createdAt'] = {type: 'Date'};
    response['updatedAt'] = {type: 'Date'};
    response['objectId'] = {type: 'String'};
    return response;
}

export function mongoSchemaToParseSchema(mongoSchema) {
    let clps = defaultCLPS;
    if (mongoSchema._metadata && mongoSchema._metadata.class_permissions) {
        clps = {...emptyCLPS, ...mongoSchema._metadata.class_permissions};
    }
    return {
        className: mongoSchema._id,
        fields: mongoSchemaFieldsToParseSchemaFields(mongoSchema),
        classLevelPermissions: clps,
    };
}

export function _mongoSchemaQueryFromNameQuery(name : string, query = {}) : Object {
    const object = { _id: name };
    if (query) {
        Object.keys(query).forEach(key => {
            object[key] = query[key];
        });
    }
    return object;
}

export function parseFieldTypeToMongoFieldType({ type, targetClass = null }) {
    switch (type) {
        case 'Pointer':  return `*${targetClass}`;
        case 'Relation': return `relation<${targetClass}>`;
        case 'Number':   return 'number';
        case 'String':   return 'string';
        case 'Boolean':  return 'boolean';
        case 'Date':     return 'date';
        case 'Object':   return 'object';
        case 'Array':    return 'array';
        case 'GeoPoint': return 'geopoint';
        case 'File':     return 'file';
        case 'Bytes':    return 'bytes';
    }
}

export class SchemaPartition extends Partition {

    _fetchAllSchemasFrom_SCHEMA() {
        return this.find().then(
            schemas => schemas.map(mongoSchemaToParseSchema)
        ).catch(
            error => { throw error }
        );
    }

    _fetchOneSchemaFrom_SCHEMA(name: string) : Promise {
        let query = _mongoSchemaQueryFromNameQuery(name);
        
        return this.find(query, { limit: 1 }).then(
            result => {
                if (result.length === 1) {
                    return mongoSchemaToParseSchema(result[0]);
                } else {
                    throw undefined;
                }
            }
        );
    }

    findAndDeleteSchema(name : string) : Promise {
        return this.deleteOne({ _id : name });
    }

    updateSchema(name: string, update: Object) : Promise {
        let _query = _mongoSchemaQueryFromNameQuery(name);
        return this.updateOne(_mongoSchemaQueryFromNameQuery(name), update);
    }

    upsertSchema(name: string, query: Object, update: Object) : Promise {
        return this.upsertOne(_mongoSchemaQueryFromNameQuery(name, query), update);
    }

    addFieldIfNotExists(className: string, fieldName: string, type: sType) : Promise {
        return this.upsertSchema(
            className,
            { [fieldName]: { '$exists': false } },
            { '$set' : { [fieldName]: parseFieldTypeToMongoFieldType(type) } }
        );
    }
}