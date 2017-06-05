// Partition is the class where objects are stored
// Partition is like Collection in MongoDB
// a Partition is represented by a hash or partition key in DynamoDB

import { DynamoDB } from 'aws-sdk';
import * as Promise from 'bluebird';


var u = require('util');

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
    KeyConditionExpression : string = '[prev]';
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

        let _key = key.replace(/^(_|\$)+/, '');

        let exp : string;

        this.ExpressionAttributeNames['#' + _key] = key;
        let index = Object.keys(this.ExpressionAttributeValues).filter(
            e => {
                if (e.indexOf(':' + key) === 0) {
                    return e;
                }
            }
        ).length;
        this.ExpressionAttributeValues[':' + _key + '_' + index] = value;

        switch (op) {
            case 'begins_with':
                exp = '( begins_with([key], [value]) )';
                break;
            case 'attribute_exists':
                exp = '( attribute_exists([key]) )';
                delete this.ExpressionAttributeValues[':' + _key + '_' + index];
                break;
            case 'attribute_not_exists':
                exp = '( attribute_not_exists([key]) )';
                delete this.ExpressionAttributeValues[':' + _key + '_' + index];
                break;
            case 'contains':
                exp = '( contains([key], [value]) )';   
                break;
            case 'IN':
                let _v = this.ExpressionAttributeValues[':' + _key + '_' + index];
                if (_v.indexOf(null) > -1 || _v.indexOf(undefined) > -1) {
                    if (_v.length == 2) {
                        let _k = ':' + _key + '_' + index;
                        this.ExpressionAttributeValues[_k] = _v.sort()[0];
                        exp = '( [key] = [value] OR attribute_not_exists([key]) )';
                    } else {
                        let _vs = [];
                        _v = _v.filter(e => e != null);
                        _v.forEach(
                            (e,i) => {
                                let _k = ':' + _key + '_' + index + '_' + i;
                                this.ExpressionAttributeValues[_k] = e;
                                _vs.push(_k);
                            }
                        )
                        exp = '( [key] IN [value] OR attribute_not_exists([key]) )'.replace('[value]', '(' + _vs.join() + ')');
                    }
                } else {
                    let _vs = [];
                    _v = _v.filter(e => e != null);
                    _v.forEach(
                        (e,i) => {
                            let _k = ':' + _key + '_' + index + '_' + i;
                            this.ExpressionAttributeValues[_k] = e;
                            _vs.push(_k);
                        }
                    )
                    exp = '( [key] IN [value] )'.replace('[value]', '(' + _vs.join() + ')');
                }
                break;
            default:
                exp = '( [key] [op] [value] )';
                break;
        }

        exp = exp.replace(/\[key\]/g, '#' + _key);
        exp = exp.replace(/\[value\]/g, ':' + _key + '_' + index);
        exp = exp.replace(/\[op\]/g, op);

        if (not) {
            exp = '( NOT ' + exp + ' )';
        }

        return exp;
    }

    build(query = {}, key = null, not = false, _op = null) : FilterExpression {
        console.log('Query', query);
        let exp, _cmp_;
        Object.keys(query).forEach(
            (q,i) => {
                switch(q) {
                    case '$nor':
                        throw "Operator not supported";
                    case '$or':
                    case '$and':
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
                        exp = this.createExp(key, query[q], _cmp_, false);
                        this.FilterExpression = this.FilterExpression.replace('[prev]', exp);
                        this.FilterExpression = this.FilterExpression.replace('[next]', '[prev]');
                        break;
                    case '$in':
                        let list = query[q] || [];
                        if (list.length === 0) throw "$in cannot be empty";
                        if (list.length === 1) {
                            query[key] = query[q][0];
                            delete query[q];
                            this.build(query, key, not, _op);
                        } else {
                            exp = this.createExp(key, query[q], 'IN', false);
                            this.FilterExpression = this.FilterExpression.replace('[prev]', exp);
                        }
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
                    case '_id':
                        break;
                    default:
                        if (query[q] instanceof Object) {
                            if (i < (Object.keys(query).length - 1)) {
                                this.FilterExpression = this.FilterExpression.replace('[prev]','( [prev] AND [prev] )');
                            }
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

    buildKC(query = {}, key = null, not = false, _op = null) : FilterExpression {
        console.log('Query', query);
        let exp, _cmp_;
        Object.keys(query).forEach(
            (q,i) => {
                switch(q) {
                    case '$nor':
                        throw "Operator not supported";
                    case '$or':
                    case '$and':
                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[prev]','( [prev] ' + this.comperators[q] + ' [next] )');
                        query[q].forEach(
                            subquery => {
                                this.buildKC(subquery, key, not, q);
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
                        exp = this.createExp(key, query[q], _cmp_, false);
                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[prev]', exp);
                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[next]', '[prev]');
                        break;
                    case '$in':
                        let list = query[q] || [];
                        if (list.length === 0) throw "$in cannot be empty";
                        if (list.length === 1) {
                            query[key] = query[q][0];
                            delete query[q];
                            this.buildKC(query, key, not, _op);
                        } else {
                            exp = this.createExp(key, query[q], 'IN', false);
                            this.KeyConditionExpression = this.KeyConditionExpression.replace('[prev]', exp);
                        }
                        break;
                    case '$nin':
                        exp = this.createExp(key, query[q], 'IN', true);
                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[prev]', exp);
                        break;
                    case '$regex':
                        _cmp_ = query[q].startsWith('^') ? 'begins_with' : 'contains';
                        exp = this.createExp(key, query[q], _cmp_, not);
                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[prev]', exp);
                        break;
                    case '$exists':
                        _cmp_ = query[q] ? 'attribute_exists' : 'attribute_not_exists';
                        exp = this.createExp(key, query[q], _cmp_, not);
                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[prev]', exp);
                        break;
                    case '$not':
                        this.buildKC(query[q], key, true, _op);
                        break;
                    case '_id':
                        console.log('_id ->', query[q], q, query);
                        if (query[q] instanceof Object) {
                            this.buildKC(query[q], q, not, _op);
                        } else {
                            exp = this.createExp(q, query[q], '=', not);
                            this.KeyConditionExpression = this.KeyConditionExpression.replace('[prev]', exp);
                            this.KeyConditionExpression = this.KeyConditionExpression.replace('[next]', '[prev]');
                        }
                        break;
                    default:
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

    constructor(database : string, className : string, service : DynamoDB) {
        this.dynamo = new DynamoDB.DocumentClient({ service : service });
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
        keys = keys || [];
        let params : DynamoDB.DocumentClient.GetItemInput = {
            TableName : this.database,
            Key: {
                _pk_className : this.className,
                _id : id
            }
        }

        if (keys.length > 0) {
            params.AttributesToGet = keys;
        }

        console.log('get', u.inspect(params, false, null));

        return new Promise(
            (resolve, reject) => {
                this.dynamo.get(params, (err, data) => {
                    if (err) {
                        console.log('err get', err);
                        reject(err);
                    } else {
                        console.log('result get', data);
                        if (data.Item) {
                            delete data.Item._pk_className;
                        }
                        resolve([data.Item]);
                    }
                })
            }
        );
    }

    _query(query: Object = {}, options : Options = {}, params : DynamoDB.DocumentClient.QueryInput = null, results = []) : Promise {
        
        const between = (n, a, b) => {
            return (n - a) * (n - b) <= 0
        }

        if (!params) {
            let keys = Object.keys(options.keys || {});
            let count = options.count ? true : false;

            // maximum by DynamoDB is 100 or 1MB
            console.log('limit1', options.limit, between(options.limit, 1, 100));
            let limit = between(options.limit, 1, 100) ? options.limit : 100;
            console.log('limit2', limit);

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
                    _pk_className : this.createCondition('$eq', this.className)
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

                exp = exp.buildKC(query);
                console.log('kc_exp', exp);
                if (exp.KeyConditionExpression !== '[prev]') {
                    _params.KeyConditionExpression = '( ( #className = :className ) AND [next] )';
                    _params.KeyConditionExpression = _params.KeyConditionExpression.replace('[next]', exp.KeyConditionExpression);
                    _params.ExpressionAttributeNames['#className'] = '_pk_className';
                    _params.ExpressionAttributeValues[':className'] = this.className;
                    delete _params.KeyConditions;
                }
            }

            if (descending) {
                _params.ScanIndexForward = descending;
            }

            params = _params;
        }

        console.log('query',u.inspect(params, false, null));

        return new Promise(
            (resolve, reject) => {
                this.dynamo.query(params, (err, data) => {
                    if (err) {
                        console.log('err', err);
                        reject(err);
                    } else {
                        results = results.concat(data.Items || []);
                        if (data.LastEvaluatedKey && (results.length < options.limit)) {
                            options.limit = options.limit - results.length;
                            params.ExclusiveStartKey = data.LastEvaluatedKey;
                            return this._query(query, options, params, results);
                        }

                        if (options.count) {
                            resolve(data.Count ? data.Count : 0);
                        }
                        console.log('results', results);
                        results.forEach((item) => {
                            delete item._pk_className;
                        });
                        resolve(results);
                    }
                });
            }
        )
    }

    find(query: Object = {}, options : Options = {}) : Promise {
        if (query.hasOwnProperty('_id') && typeof query['_id'] === 'string') {
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
                _pk_className : this.className,
                _id : id,
                ...object
            }
        }

        console.log('insert', u.inspect(params, false, null));

        return new Promise(
            (resolve, reject) => {
                this.dynamo.put(params, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({
                            ok : 1,
                            n : 1,
                            ops : [ object ],
                            insertedId : id
                        });
                    }
                });
            }
        )
    }

    updateOne(query = {}, object : Object) : Promise {
        console.log('uquery', query);
        console.log('uobject', object);
        let id = query['_id'] || object['_id'];
        let params : DynamoDB.DocumentClient.UpdateItemInput = {
            TableName : this.database,
            Key: {
                _pk_className : this.className
            },
            ReturnValues : 'UPDATED_NEW'
        }

        if (id) {
            params.Key._id = id;
        } else {
            if (Object.keys(query).length > 0) {
                let exp : FilterExpression = new FilterExpression();
                exp = exp.build(query);
                params.ConditionExpression = exp.FilterExpression;
                params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
                params.ExpressionAttributeValues = exp.ExpressionAttributeValues;
            }
        }

        delete object['_id'];
        if (Object.keys(object).length > 0) {
            let $set = object, $unset = [];
            if (object.hasOwnProperty('$set')) {
                $set = object['$set'] || {};
            }

            if (object.hasOwnProperty('$unset')) {
                $unset = Object.keys((object['$unset'] || {}));
            }

            object = null; // destroy object;

            params.AttributeUpdates = {};
            for (let key in $set) {
                let action = $set[key] === undefined ? 'DELETE' : 'PUT';
                if ($set[key] instanceof Date) {
                    $set[key] = $set[key].toISOString();
                }

                params.AttributeUpdates[key] = {
                    Action : action
                }

                if (action == 'PUT') {
                    params.AttributeUpdates[key].Value = $set[key];
                }
            }

            $unset.forEach(key => {
                params.AttributeUpdates[key] = {
                    Action : 'DELETE'
                }
            });
        }

        console.log('update', u.inspect(params, false, null));

        return new Promise(
            (resolve, reject) => {
                this.dynamo.update(params, (err, data) => {
                    if (err) {
                        console.log('uerr', err);
                        reject(err);
                    } else {
                        console.log('udata', data);
                        resolve({
                            ok : 1,
                            n : 1,
                            nModified : 1,
                            value : data.Attributes
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
                                        ok : 1,
                                        n : (res || []).length
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
                _pk_className : this.className
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
                            ok : 1 ,
                            n : 1,
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
                                        ok : 1,
                                        n : (res || []).length,
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