import { DynamoDB } from 'aws-sdk';
import * as Promise from 'bluebird';
import * as Transform from 'parse-server/lib/Adapters/Storage/Mongo/MongoTransform';
import { Partition } from './DynamoPartition';
import { SchemaPartition, mongoSchemaToParseSchema, parseFieldTypeToMongoFieldType } from './SchemaPartition';
import { Parse } from 'parse/node';
import { _ } from 'lodash';

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
            }
        default: throw `no type for ${JSON.stringify(type)} yet`;
    }
};

Transform.convertParseSchemaToMongoSchema = ({...schema}) => {
    delete schema.fields._rperm;
    delete schema.fields._wperm;

    if (schema.className === '_User') {
        delete schema.fields._hashed_password;
    }

    return schema;
}

Transform.mongoSchemaFromFieldsAndClassNameAndCLP = (fields, className, classLevelPermissions) => {
    const mongoObject = {
        _id: className,
        objectId: 'string',
        updatedAt: 'string',
        createdAt: 'string'
    };

    for (const fieldName in fields) {
        mongoObject[fieldName] = parseFieldTypeToMongoFieldType(fields[fieldName]);
    }

    if (typeof classLevelPermissions !== 'undefined') {
        mongoObject['_metadata'] = mongoObject['_metadata'] || {};
        if (!classLevelPermissions) {
            delete mongoObject['_metadata'].class_permissions;
        } else {
            mongoObject['_metadata'].class_permissions = classLevelPermissions;
        }
    }

    return mongoObject;
}

export class Adapter {
    
    service : DynamoDB;
    database : string;
    settings : Object;

    constructor(database : string, settings : DynamoDB.DocumentClient.DocumentClientOptions) {
        this.database = database,
        this.settings = settings;
        this.service = new DynamoDB(this.settings);
    }

    connect() {
        return Promise.resolve();
    }

    _adaptiveCollection(name : string) : Partition {
        return new Partition(this.database, name, this.service);
    }

    _schemaCollection() : SchemaPartition {
        return new SchemaPartition(this.database, schemaTable, this.service);
    }

    classExists(name : string) : Promise {
        return this._schemaCollection().find({ _id : name }).then(
            partition => {
                return partition !== undefined;
            }
        )
    }

    setClassLevelPermissions(className, CLPs) {
        return this._schemaCollection().updateSchema(className, {}, {
             _metadata: { class_permissions: CLPs }
        });
    }

    createClass(className, schema) {
        return this.classExists(className).then(
            partition => {
                console.log('partition', partition);
                if (!partition) {
                    schema = Transform.convertParseSchemaToMongoSchema(schema);
                    const mongoObject = Transform.mongoSchemaFromFieldsAndClassNameAndCLP(schema.fields, className, schema.classLevelPermissions);
                    mongoObject._id = className;
                    return this._schemaCollection().insertOne(mongoObject)
                    .then(
                        result => {
                            return mongoSchemaToParseSchema(result.ops[0]);
                        }
                    )
                    .catch(
                        error => {
                            throw error;
                        }
                    )
                } else {
                    throw new Parse.Error(Parse.Error.DUPLICATE_VALUE, 'Class already exists.');
                }
            }
        ).catch(console.log)
    }

    addFieldIfNotExists(className, fieldName, type) {
        return this._schemaCollection().addFieldIfNotExists(className, fieldName, type);
    }

    deleteClass(className) {
        // only drop Schema!
        return this._schemaCollection().findAndDeleteSchema(className);
    }

    deleteAllClasses() {
        return Promise.reject({
            error : "operation not supported by DynamoDB"
        });
    }

    deleteFields(className, schema, fieldNames) {
        // remove fields only from Schema
        let update = {};
        fieldNames.forEach(field => {
            update[field] = undefined;
        });
        
        return this._schemaCollection().updateSchema(name, { _id : name }, update);
    }

    getAllClasses() {
        return this._schemaCollection()._fetchAllSchemasFrom_SCHEMA();
    }

    getClass(className) {
        return this._schemaCollection()._fechOneSchemaFrom_SCHEMA(className);
    }

    transformDateObject(object = {}) : Object {
        Object.keys(object).forEach(
            key => {
                if (object[key] instanceof Date) {
                    object[key] = object[key].toISOString();
                }

                if (object[key] instanceof Object) {
                    if (object[key].hasOwnProperty('__type')) {
                        if ((object[key].__type || "").toLowerCase() == 'date') {
                            console.log('date iso',object[key], object[key].iso);
                            object[key] = new Date(object[key].iso || new Date());
                            try {
                                object[key] = object[key].toISOString();
                            } catch(err) {
                                throw err;
                            }
                        }
                    } else {
                        object[key] = this.transformDateObject(object[key]);
                    }
                }
            }
        )

        return object;
    }

    createObject(className, schema, object) {
        object = this.transformDateObject(object);
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        object = Transform.parseObjectToMongoObjectForCreate(className, object, schema);
        Object.keys(object).forEach(
            key => {
                if (object[key] instanceof Date) {
                    object[key] = object[key].toISOString();
                }
            }
        )
        return this._adaptiveCollection(className).insertOne(object)
            .catch(
                (error) => {
                    throw error;
                }
            )
    }

    deleteObjectsByQuery(className, schema, query) {
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        query = Transform.transformWhere(className, query, schema);

        return this._adaptiveCollection(className).deleteMany(query)
            .then(
                result => {
                    if (result.n === 0) {
                        throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found.');
                    }
                    return Promise.resolve();
                }
            )
            .catch(
                error => {
                    throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, 'Database adapter error');
                }
            )
    }

    updateObjectsByQuery(className, schema, query, update) {
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        update = Transform.transformUpdate(className, update, schema);
        query = Transform.transformWhere(className, query, schema);

        return this._adaptiveCollection(className).updateMany(query, update);
    }

    findOneAndUpdate(className, schema, query, update) {
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        update = Transform.transformUpdate(className, update, schema);
        query = Transform.transformWhere(className, query, schema);

        return this._adaptiveCollection(className).updateOne(query, update)
            .then(result => Transform.mongoObjectToParseObject(className, result.value, schema));
    }

    upsertOneObject(className, schema, query, update) {
        return this.findOneAndUpdate(className, schema, query, update);
    }

    find(className, schema, query, { skip, limit, sort, keys }) {
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        query = Transform.transformWhere(className, query, schema);
        sort = _.mapKeys(sort, (value, fieldName) => Transform.transformKey(className, fieldName, schema));
        keys = _.reduce(keys, (memo, key) => {
            memo[Transform.transformKey(className, key, schema)] = 1;
            return memo;
        }, {});
        console.log('options ->', { skip, limit, sort, keys});
        return this._adaptiveCollection(className).find(query, { skip, limit, sort, keys})
            .then(
                objects => objects.map(object => Transform.mongoObjectToParseObject(className, object, schema))
            )
    }

    ensureUniqueness(className, schema, fieldNames) {
        return Promise.resolve();
    }

    count(className, schema, query) {
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        return this._adaptiveCollection(className).count(Transform.transformWhere(className, query, schema));
    }

    performInitialization() {
        return Promise.resolve();
    }
}