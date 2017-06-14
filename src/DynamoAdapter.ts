import { DynamoDB } from 'aws-sdk';
import * as Promise from 'bluebird';
import * as Transform from 'parse-server/lib/Adapters/Storage/Mongo/MongoTransform';
import { Partition } from './DynamoPartition';
import { SchemaPartition, mongoSchemaToParseSchema, parseFieldTypeToMongoFieldType } from './SchemaPartition';
import { Parse } from 'parse/node';
import { _ } from 'lodash';

type Options = {
    skip? : Object, // not supported
    limit? : number,
    sort? : Object, // only supported on partition/sort key
    keys? : Object
    count?: boolean
}

const schemaTable = '_SCHEMA';

// not used at the moment but can be helpful in the future!
const DynamoType = type => {
    switch (type.type) {
        case 'String': return 'S'; // string
        case 'Date': return 'S';   // string
        case 'Object': return 'M'; // object
        case 'File': return 'M';   // object
        case 'Boolean': return 'BOOL'; // boolean
        case 'Pointer': return 'S'; // string
        case 'Relation' : return 'M'; // string
        case 'Number': return 'N'; // number
        case 'GeoPoint': return 'M'; // object
        case 'Array':
            if (type.contents && type.contents.type === 'String') {
                return 'SS'; // string[]
            } else if (type.contents && type.contents.type === 'Number') {
                return 'NS'; // number[]
            } else {
                return 'L'; // array
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

    connect() : Promise {
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
            partition => partition.length > 0
        ).catch(
            error => { throw error }
        );
    }

    setClassLevelPermissions(className, CLPs) : Promise {
        return this._schemaCollection().updateSchema(className, {}, {
             _metadata: { class_permissions: CLPs }
        }).catch(
            error => { throw error }
        );
    }

    createClass(className, schema) : Promise {
        return this.classExists(className).then(
            partition => {
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
        ).catch(
            error => { throw error }
        );
    }

    addFieldIfNotExists(className, fieldName, type) : Promise {
        return this._schemaCollection().addFieldIfNotExists(className, fieldName, type)
            .catch(
                error => { throw error }
            );
    }

    deleteClass(className) : Promise {
        // only drop Schema!
        return this._schemaCollection().findAndDeleteSchema(className)
            .catch(
                error => { throw error }
            );
    }

    deleteAllClasses() : Promise {
        // only for test
        let exec = require('child_process').execSync;
        return this.service.describeTable({ TableName : this.database })
            .promise()
                .catch(() => {
                    return exec('aws dynamodb delete-table --table-name parse-server --endpoint http://localhost:8000');
                })
                .catch(() => {
                    return exec('aws dynamodb create-table --table-name parse-server --attribute-definitions AttributeName=_pk_className,AttributeType=S AttributeName=_sk_id,AttributeType=S --key-schema AttributeName=_pk_className,KeyType=HASH AttributeName=_sk_id,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 --endpoint-url http://localhost:8000');
                })
                .catch(() => {
                    return Promise.resolve();
                });
    }

    deleteFields(className, schema, fieldNames) : Promise {
        // remove fields only from Schema
        let update = {};
        fieldNames.forEach(field => {
            update[field] = undefined;
        });
        
        return this._schemaCollection().updateSchema(name, { _id : name }, update)
            .catch(
                error => { throw error }
            );
    }

    getAllClasses() : Promise {
        return this._schemaCollection()._fetchAllSchemasFrom_SCHEMA();
    }

    getClass(className) : Promise {
        return this._schemaCollection()._fechOneSchemaFrom_SCHEMA(className)
            .catch(
                error => { throw error }
            );
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

    createObject(className, schema, object) : Promise {
        object = this.transformDateObject(object);
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        object = Transform.parseObjectToMongoObjectForCreate(className, object, schema);
        object = object = this.transformDateObject(object);
        Object.keys(object).forEach(
            key => {
                if (object[key] instanceof Date) {
                    object[key] = object[key].toISOString();
                }
            }
        );

        return this._adaptiveCollection(className).insertOne(object)
            .catch(
                error => { throw error }
            );
    }

    deleteObjectsByQuery(className, schema, query) : Promise {
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        query = Transform.transformWhere(className, query, schema);
        query = this.transformDateObject(query);

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
                error => { throw error }
            );
    }

    updateObjectsByQuery(className, schema, query, update) {
        update = this.transformDateObject(update);
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        update = Transform.transformUpdate(className, update, schema);
        update = this.transformDateObject(update);
        query = Transform.transformWhere(className, query, schema);
        query = this.transformDateObject(query);

        return this._adaptiveCollection(className).updateMany(query, update)
            .catch(
                error => { throw error }
            );
    }

    findOneAndUpdate(className, schema, query, update) {
        update = this.transformDateObject(update);
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        update = Transform.transformUpdate(className, update, schema);
        update = this.transformDateObject(update);
        query = Transform.transformWhere(className, query, schema);
        query = this.transformDateObject(query);

        return this._adaptiveCollection(className).updateOne(query, update)
            .then(result => Transform.mongoObjectToParseObject(className, result.value, schema))
            .catch(
                error => { throw error }
            );
    }

    upsertOneObject(className, schema, query, update) {
        return this.findOneAndUpdate(className, schema, query, update);
    }

    find(className, schema = {}, query = {}, options : Options = {}) {
        let { skip, limit, sort, keys } = options;
        schema = Transform.convertParseSchemaToMongoSchema(schema);
        query = Transform.transformWhere(className, query, schema);
        query = this.transformDateObject(query);
        sort = _.mapKeys(sort, (value, fieldName) => Transform.transformKey(className, fieldName, schema));
        keys = _.reduce(keys, (memo, key) => {
            memo[Transform.transformKey(className, key, schema)] = 1;
            return memo;
        }, {});
        
        return this._adaptiveCollection(className).find(query, { skip, limit, sort, keys})
            .then(
                objects => objects.map(object => Transform.mongoObjectToParseObject(className, object, schema))
            )
            .catch(
                error => { throw error }
            );
    }

    _rawFind(className, query = {}) {
        return this._adaptiveCollection(className).find(query)
            .catch(
                error => { throw error }
            );
    }

    ensureUniqueness(className, schema, fieldNames) {
        return Promise.resolve();
    }

    count(className, schema, query) {
        schema = Transform.convertParseSchemaToMongoSchema(schema);

        return this._adaptiveCollection(className).count(Transform.transformWhere(className, query, schema))
            .catch(
                error => { throw error }
            );
    }

    performInitialization() {
        return Promise.resolve();
    }
}