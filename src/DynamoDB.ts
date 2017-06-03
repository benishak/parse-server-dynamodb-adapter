import { DynamoDB } from 'aws-sdk';
import * as Promise from 'bluebird';
import * as MongoTransform from 'parse-server/lib/Adapters/Storage/Mongo/MongoTransform';

const READCAPACITYU : number = 5;
const WRITECAPACITYU : number = 5;
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

const DynamoComperator = {
    '$gt': 'GT',
    '$lt': 'LT',
    '$gte': 'GE',
    '$lte': 'LE',
    '$eq' : 'EQ',
    '$ne' : 'NE',
    '$in' : 'IN'
}

export class DynamoDBAdapter {
    
    db : DynamoDB;
    settings : Object;

    constructor(apiVersion : string, settings : Object = {}) {
        this.db = new DynamoDB({apiVersion: apiVersion});
        this.settings = settings;
        this.settings['ReadCapacityUnits'] = READCAPACITYU || this.settings['ReadCapacityUnits'];
        this.settings['ReadCapacityUnits'] = WRITECAPACITYU || this.settings['WriteCapacityUnits'];
    }

    transformSchema(schema) {

    }

    transformObject(object) {
        // DynamoDB doesn't allow double underscore '__'
        for (let key of object) {
            switch(key) {
                case 'createdAt':
                case 'updatedAt':
                case 'expiresAt':
                    object[key]._type = 'Date';
                    delete object[key].__type;
                    break;
            }

            if (key.startsWith('__')) {
                let _key = key
                object[]
            }

            if (key && (object[key] === 'Pointer' || object[key] === 'Relation')) {
                object[key]._type = object[key]._type;
                delete object[key].__type;
            }
        }
    }

    createTable(className : string, schema) : Promise {
        return new Promise(
            (resolve, reject) => {
                const attributes = schema.fields;
                const params = {
                    TableName : className,
                    KeySchema: [
                        { AttributeName: "id", KeyType: "HASH"},
                    ],
                    AttributeDefinitions: [       
                        { AttributeName: "id", AttributeType: "S" },
                        ...attributes
                    ],
                    ProvisionedThroughput: {       
                        ReadCapacityUnits: this.settings['ReadCapacityUnits'],
                        WriteCapacityUnits: this.settings['WriteCapacityUnits']
                    }
                }

                this.db.createTable(params, (err, data) => {
                    if (err) {
                        reject(err)
                    } else {

                    }
                })
            }
        )
    }
} 