// Partition is the class where objects are stored
// Partition is like Collection in MongoDB
// a Partition is represented by a hash or partition key in DynamoDB

import { DynamoDB } from 'aws-sdk';
import * as Promise from 'bluebird';

type Options = {
    skip? : Object, // not supported
    limit? : number,
    sort? : Object, // only supported on partition/sort key
    keys? : Object
    count?: boolean
}

const DynamoComperator = {
    '$gt': 'GT',
    '$lt': 'LT',
    '$gte': 'GE',
    '$lte': 'LE',
    '$eq' : 'EQ',
    '$ne' : 'NE',
    '$in' : 'IN',
    '$exists' : 'NOT_NULL',
    '$regex' : 'BEGINS_WITH',
}

// helper class to generate DynamoDB FilterExpression from MongoDB Query Object
export class FilterExpression {

    FilterExpression : string = '[prev]';
    ExpressionAttributeValues = {};
    ExpressionAttributeNames = {};

    comperators = {
        '$gt': '>',
        '$lt': '<',
        '$gte': '>=',
        '$lte': '<=',
        '$eq' : '=',
        '$ne' : '<>',
        '$in' : 'IN',
        '$or' : 'OR',
        '$and' : 'AND',
        '$not' : 'NOT'
    }

    __not = {
        '$gt': '<',
        '$lt': '>',
        '$gte': '<=',
        '$lte': '>=',
        '$eq' : '<>',
        '$ne' : '='
    }

    constructor() {}

    createExp(key, value, op, not = false) {
        if (!op) {
            throw "Operator not supported";
        }

        let exp : string;

        this.ExpressionAttributeNames['#' + key] = key;
        let index = Object.keys(this.ExpressionAttributeValues).filter(
            e => {
                if (e.indexOf(':' + key) === 0) {
                    return e;
                }
            }
        ).length;
        this.ExpressionAttributeValues[':' + key + '_' + index] = value;

        switch (op) {
            case 'begins_with':
                exp = '( begins_with([key], [value]) )';
                break;
            case 'attribute_exists':
                exp = '( attribute_exists([key]) )';
                break;
            case 'attribute_not_exists':
                exp = '( attribute_not_exists([key]) )';
                break;
            case 'contains':
                exp = '( contains([key], [value]) )';
                break;
            case 'IN':
                this.ExpressionAttributeValues[':' + key + '_' + index] = '(' + this.ExpressionAttributeValues[':' + key + '_' + index].join() + ')';
                exp = '( IN [value] )';
                break;
            default:
                exp = '( [key] [op] [value] )';
                break;
        }

        exp = exp.replace('[key]', '#' + key);
        exp = exp.replace('[value]', ':' + key + '_' + index);
        exp = exp.replace('[op]', op);

        if (not) {
            exp = '( NOT ' + exp + ' )';
        }

        return exp;
    }

    build(query = {}, key = null, not = false, _op = null) : FilterExpression {
        let exp;
        let _cmp_;
        Object.keys(query).forEach(
            q => {
                switch(q) {
                    case '$nor':
                        throw "Operator not supported";
                    case '$or':
                    case '$and' :
                        this.FilterExpression = this.FilterExpression.replace('[prev]','( [prev] ' + this.comperators[q] + ' [next] )');
                        query[q].forEach(
                            subquery => {
                                this.build(subquery, key, not, q);
                            }
                        )
                        break;
                    case '$eq':
                    case '$ne':
                    case '$gt':
                    case '$lt':
                    case '$gte':
                    case '$lte':
                        _cmp_ = not ? this.__not[q] : this.comperators[q];
                        let exp = this.createExp(key, query[q], _cmp_, false);
                        this.FilterExpression = this.FilterExpression.replace('[prev]', exp);
                        this.FilterExpression = this.FilterExpression.replace('[next]', '[prev]');
                        break;
                    case '$in':
                        exp = this.createExp(key, query[q], 'IN', false);
                        this.FilterExpression = this.FilterExpression.replace('[prev]', exp);
                        break;
                    case '$nin':
                        exp = this.createExp(key, query[q], 'IN', true);
                        this.FilterExpression = this.FilterExpression.replace('[prev]', exp);
                        break;
                    case '$regex':
                        _cmp_ = query[q].startsWith('^') ? 'begins_with' : 'contains';
                        exp = this.createExp(key, query[q], _cmp_, not);
                        this.FilterExpression = this.FilterExpression.replace('[prev]', exp);
                        break;
                    case '$exists':
                        _cmp_ = query[q] ? 'attribute_exists' : 'attribute_not_exists';
                        exp = this.createExp(key, query[q], _cmp_, not);
                        this.FilterExpression = this.FilterExpression.replace('[prev]', exp);
                        break;
                    case '$not':
                        this.build(query[q], key, true, _op);
                        break;
                    default:
                        if (query[q] instanceof Object) {
                            this.build(query[q], q, not, _op);
                        } else {
                            exp = this.createExp(q, query[q], '=', not);
                            this.FilterExpression = this.FilterExpression.replace('[prev]', exp);
                            this.FilterExpression = this.FilterExpression.replace('[next]', '[prev]');
                        }
                        break;
                }
            }
        )

        return this;
    }
}

export class Partition {
    database : string; // database is the table name in DynamoDB
    className : string;
    dynamo : DynamoDB.DocumentClient;

    constructor(database : string, className : string, settings : DynamoDB.DocumentClient.DocumentClientOptions) {
        this.dynamo = new DynamoDB.DocumentClient();
        this.database = database;
        this.className = className;
    }

    createCondition (comperator : string, value) : DynamoDB.DocumentClient.Condition {
        let condition : DynamoDB.DocumentClient.Condition = {
            ComparisonOperator : DynamoComperator[comperator] || "EQ",
            AttributeValueList : [ value ]
        }

        if (comperator === '$exists') {
            if (value === true) {
                condition.ComparisonOperator = "NULL";
            }
            delete condition.AttributeValueList;
        }

        return condition;
    }

    getDynamoQueryFilter(query : Object = {}) : DynamoDB.DocumentClient.FilterConditionMap {
        let QueryFilter : DynamoDB.DocumentClient.FilterConditionMap = {};

        for (let key in query) {
            let cmp = Object.keys(query[key])[0];
            QueryFilter[key] = this.createCondition(
                cmp,
                query[key][cmp] || query[key]
            )
        }

        return QueryFilter;
    }

    _get(id : string, keys : string[] = []) : Promise {
        let params : DynamoDB.DocumentClient.GetItemInput = {
            TableName : this.database,
            Key: {
                className : this.className,
                _id : id
            },
            AttributesToGet : keys
        }

        return new Promise(
            (resolve, reject) => {
                this.dynamo.get(params, (err, data) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(data.Item);
                    }
                })
            }
        );
    }

    _query(query: Object = {}, options : Options = {}, params : DynamoDB.DocumentClient.QueryInput = null, results = []) : Promise {
        if (!params) {
            let keys = Object(options.keys || {}).keys();
            let count = options.count ? true : false;

            // maximum by DynamoDB is 100 or 1MB
            let limit = options.limit in [1, 100] ? options.limit : 100;

            // DynamoDB sorts only by sort key (in our case the objectId
            options.sort = options.sort || {};
            let descending = false;
            if (options.sort.hasOwnProperty('_id') && options.sort['_id'] == -1) {
                descending = true;
            }

            // Select keys -> projection
            let select =  keys.length > 0 ? "SPECIFIC_ATTRIBUTES" : "ALL_ATTRIBUTES";
            if (count) {
                select = "COUNT"
            }
            
            let _params : DynamoDB.DocumentClient.QueryInput = {
                TableName : this.database,
                KeyConditions: {
                    className : this.createCondition('$eq', this.className)
                },
                Limit : limit,
                Select : select
            }

            if (keys.length > 0) {
                _params.AttributesToGet = keys;
            }

            if (Object.keys(query).length > 0) {
                //_params.QueryFilter = this.getDynamoQueryFilter(query);
                let exp : FilterExpression = new FilterExpression();
                exp = exp.build(query);
                _params.FilterExpression = exp.FilterExpression;
                _params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
                _params.ExpressionAttributeValues = exp.ExpressionAttributeValues;
            }

            if (descending) {
                _params.ScanIndexForward = descending;
            }

            params = _params;
        }

        return new Promise(
            (resolve, reject) => {
                this.dynamo.query(params, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        results = results.concat(data.Items || []);
                        if (data.LastEvaluatedKey && results.length < 100) {
                            options.limit = 100 - results.length;
                            params.ExclusiveStartKey = data.LastEvaluatedKey;
                            return this._query(query, options, params, results);
                        }

                        if (options.count) {
                            resolve(data.Count ? data.Count : 0);
                        }

                        resolve(results);
                    }
                });
            }
        )
    }

    find(query: Object = {}, options : Options = {}) : Promise {
        if (query.hasOwnProperty('_id')) {
            let id = query['_id'];
            let keys = Object.keys(options.keys || {});
            return this._get(id, keys);
        } else {
            return this._query(query, options);
        }
    }

    count(query: Object = {}, options : Options = {}) : Promise {
        options.count = true;
        return this._query(query, options);
    }

    insertOne(object) : Promise {
        let id = object['_id'];
        delete object['_id'];
        let params : DynamoDB.DocumentClient.PutItemInput = {
            TableName : this.database,
            Item: {
                className : this.className,
                _id : id,
                ...object
            }
        }

        return new Promise(
            (resolve, reject) => {
                this.dynamo.put(params, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({
                            result : { ok : 1, n : 1 },
                            ops : [ object ],
                            insertedId : id
                        });
                    }
                });
            }
        )
    }

    updateOne(query = {}, object : Object) : Promise {
        let id = query['_id'] || object['_id'];
        let params : DynamoDB.DocumentClient.UpdateItemInput = {
            TableName : this.database,
            Key: {
                className : this.className
            }
        }

        if (id) {
            params.Key._id = id;
        } else {
            delete object['_id'];
            for (let key in object) {
                params.AttributeUpdates[key] = {
                    Action : 'PUT',
                    Value : object[key]
                }
            }

            if (Object.keys(query).length > 0) {
                let exp : FilterExpression = new FilterExpression();
                exp = exp.build(query);
                params.ConditionExpression = exp.FilterExpression;
                params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
                params.ExpressionAttributeValues = exp.ExpressionAttributeValues;
            }
        }

        return new Promise(
            (resolve, reject) => {
                this.dynamo.update(params, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({
                            result : { ok : 1, n : 1, nModified : 1 }
                        });
                    }
                });
            }
        )
    }

    upsertOne(query = {}, object : Object) : Promise {
        return this.updateOne(query, object);
    }

    updateMany(query = {}, object) {
        let id = query['_id'] || object['_id'];

        if (id) {
            return this.updateOne(query, object);
        } else {let options = {
                limit : 25,
                keys : { _id : 1 }
            }
            return this.find(query, options).then(
                (res) => {
                    let params : DynamoDB.DocumentClient.BatchWriteItemInput = {
                        RequestItems : {}
                    }

                    params.RequestItems[this.database] = res.map(item => {
                        return {
                            PutRequest : {
                                Item : {
                                    _id : item._id,
                                    ...object
                                }
                            }
                        }
                    });

                    return new Promise(
                        (resolve, reject) => {
                            this.dynamo.batchWrite(params, (err, data) => {
                                if (err) {
                                    reject(err)
                                } else {
                                    resolve({
                                        result : { ok : 1, n : (res || []).length }
                                    });
                                }
                            });
                        }
                    )
                }
            )
        }
    }

    deleteOne(query = {}) : Promise {
        let id = query['_id'];
        let params : DynamoDB.DocumentClient.DeleteItemInput = {
            TableName : this.database,
            Key: {
                className : this.className
            }
        }

        if (id) {
            params.Key._id = id;
        } else {
            if (Object.keys(query).length > 0) { 
                let exp = new FilterExpression();
                exp = exp.build(query);
                params.ConditionExpression = exp.FilterExpression;
                params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
                params.ExpressionAttributeValues = exp.ExpressionAttributeValues;
            }
        }

        return new Promise(
            (resolve, reject) => {
                this.dynamo.delete(params, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({
                            result : { ok : 1 , n : 1 },
                            deletedCount : 1
                        })
                    }
                });
            }
        )
    }

    deleteMany(query = {}) : Promise {
        let id = query['_id'];
        if (id) {
            return this.deleteOne(query);
        } else {
            let options = {
                limit : 25,
                keys : { _id : 1 }
            }
            return this.find(query, options).then(
                (res) => {
                    let params : DynamoDB.DocumentClient.BatchWriteItemInput = {
                        RequestItems : {}
                    }

                    params.RequestItems[this.database] = res.map(item => {
                        return {
                            DeleteRequest : {
                                Key : {
                                    _id : item._id
                                }
                            }
                        }
                    });

                    return new Promise(
                        (resolve, reject) => {
                            this.dynamo.batchWrite(params, (err, data) => {
                                if (err) {
                                    reject(err)
                                } else {
                                    resolve({
                                        result : { ok : 1 , n : (res || []).length },
                                        deletedCount : (res || []).length
                                    });
                                }
                            });
                        }
                    )
                }
            )
        }
    }

    _ensureSparseUniqueIndexInBackground(indexRequest) {
        return Promise.resolve();
    }

    drop () {
        return Promise.reject();
    }
}