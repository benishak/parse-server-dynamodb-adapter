// Partition is the class where objects are stored
// Partition is like Collection in MongoDB
// a Partition is represented by a hash or partition key in DynamoDB

import { DynamoDB } from 'aws-sdk';
import * as Promise from 'bluebird';
import { Parse } from 'parse/node';
var u = require('util'); // for debugging;

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

    FilterExpression : string = '[first]';
    KeyConditionExpression : string = '[first]';
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
        '$not' : 'NOT',
        '$exists' : '*',
        '$regex' : '*',
        '$nin' : '*',
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

    isQuery(object : Object = {}) {
        let $ = true;
        for (let key in object) {
            $ = $ && (this.comperators[key] != undefined)
        }
        return $;
    }

    createExp(key, value, op, not = false) {
        
        if (!op) {
            throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : Operation is not supported');
        }

        let _key = key.replace(/^(_|\$)+/, '');

        let exp : string;

        this.ExpressionAttributeNames['#' + _key] = key;
        let index = Object.keys(this.ExpressionAttributeValues).filter(
            e => {
                if (e.indexOf(':' + _key) === 0) {
                    return e;
                }
            }
        ).length;

        this.ExpressionAttributeValues[':' + _key + '_' + index] = value;

        switch (op) {
            case 'begins_with':
                exp = 'begins_with([key], [value])';
                break;
            case 'attribute_exists':
                exp = 'attribute_exists([key])';
                delete this.ExpressionAttributeValues[':' + _key + '_' + index];
                break;
            case 'attribute_not_exists':
                exp = 'attribute_not_exists([key])';
                delete this.ExpressionAttributeValues[':' + _key + '_' + index];
                break;
            case 'contains':
                exp = 'contains([key], [value])';   
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
                    exp = '[key] IN [value]'.replace('[value]', '(' + _vs.join() + ')');
                }
                break;
            default:
                exp = '[key] [op] [value]';
                break;
        }

        exp = exp.replace(/\[key\]/g, '#' + _key);
        exp = exp.replace(/\[value\]/g, ':' + _key + '_' + index);
        exp = exp.replace(/\[op\]/g, op);

        if (not) {
            exp = 'NOT ( ' + exp + ' )';
        }

        return exp;
    }

    build(query = {}, key = null, not = false, _op = null) : FilterExpression {
        let exp, _cmp_;
        Object.keys(query).forEach(
            (q,i) => {

                if (query['_id']) {
                    delete query['_id'];
                }

                if (i < Object.keys(query).length - 1 && Object.keys(query).length > 1) {
                    if (_op) {
                        this.FilterExpression = this.FilterExpression.replace('[first]','( [first] AND [next] )');
                    } else {
                        this.FilterExpression = this.FilterExpression.replace('[first]','[first] AND [next]');
                    }
                }

                if (i < Object.keys(query).length) {
                     this.FilterExpression = this.FilterExpression.replace('[next]', '[first]');
                }

                switch(q) {
                    case '$nor':
                        throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : Operator [' + q + '] not supported');
                    case '$or':
                    case '$and':
                        query[q].forEach(
                            (subquery,j) => {
                                if (j < Object.keys(query[q]).length - 1 && Object.keys(query[q]).length > 1) {
                                    if (_op == '$and') {
                                        this.FilterExpression = this.FilterExpression.replace('[first]','( [first] ' + this.comperators[q] + ' [next] )');
                                    } else {
                                        this.FilterExpression = this.FilterExpression.replace('[first]','[first] ' + this.comperators[q] + ' [next]');
                                    }
                                }
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
                        this.FilterExpression = this.FilterExpression.replace('[first]', exp);
                        break;
                    case '$in':
                    case '$nin':
                        let list = query[q] || [];
                        if (list.length === 0) throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : [$in] cannot be empty');
                        if (list.length === 1) {
                            query[key] = query[q][0];
                            delete query[q];
                            this.build(query, key, not, _op);
                        } else {
                            not = q == '$nin' ? true : not;
                            exp = this.createExp(key, query[q], 'IN', not);
                            this.FilterExpression = this.FilterExpression.replace('[first]', exp);
                        }
                        break;
                    case '$regex':
                        _cmp_ = query[q].startsWith('^') ? 'begins_with' : 'contains';
                        query[q] = query[q].replace('^', '');
                        query[q] = query[q].replace('\\Q', '');
                        query[q] = query[q].replace('\\E', '');
                        exp = this.createExp(key, query[q], _cmp_, not);
                        this.FilterExpression = this.FilterExpression.replace('[first]', exp);
                        break;
                    case '$exists':
                        _cmp_ = query[q] ? 'attribute_exists' : 'attribute_not_exists';
                        exp = this.createExp(key, query[q], _cmp_, not);
                        this.FilterExpression = this.FilterExpression.replace('[first]', exp);
                        break;
                    case '$not':
                        this.build(query[q], key, true, _op);
                        break;
                    case '_id':
                        break;
                    default:
                        if (query[q].constructor === Object && this.isQuery(query[q])) {
                            this.build(query[q], q, not, _op);
                        } else {
                            exp = this.createExp(q, query[q], '=', not);
                            this.FilterExpression = this.FilterExpression.replace('[first]', exp);
                        }
                        break;
                }
            }
        )

        return this;
    }

    buildKC(query = {}, key = null, not = false, _op = null) : FilterExpression {
        let exp, _cmp_;

        if (Object.keys(query).length > 1 && query['_id']) {
            query = {
                _id : query['_id']
            }
        }

        Object.keys(query).forEach(
            (q,i) => {

                if (i < (Object.keys(query).length - 1) && Object.keys(query).length > 1) {
                    if (_op) {
                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[first]','( [first] AND [next] )');
                    } else {
                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[first]','[first] AND [next]');
                    }
                }

                if (i < Object.keys(query).length) {
                    this.KeyConditionExpression = this.KeyConditionExpression.replace('[next]', '[first]');
                }

                switch(q) {
                    case '$ne':
                    case '$in':
                    case '$nin':
                    case '$or':
                    case '$nor':
                    case '$not':
                    case '$exists':
                        throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : Cannot apply [' + q + '] on objectId');
                    case '$and':
                        query[q].forEach(
                            (subquery,j) => {
                                if (j < Object.keys(query[q]).length - 1 && Object.keys(query[q]).length > 1) {
                                    if (_op == '$and') {
                                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[first]','( [first] ' + this.comperators[q] + ' [next] )');
                                    } else {
                                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[first]','[first] ' + this.comperators[q] + ' [next]');
                                    }
                                }
                                this.buildKC(subquery, key, not, q);
                            }
                        )
                        break;
                    case '$eq':
                    case '$gt':
                    case '$lt':
                    case '$gte':
                    case '$lte':
                        _cmp_ = not ? this.__not[q] : this.comperators[q];
                        exp = this.createExp(key, query[q], _cmp_, false);
                        this.KeyConditionExpression = this.KeyConditionExpression.replace('[first]', exp);
                        break;
                    case '$regex':
                        _cmp_ = query[q].startsWith('^') ? 'begins_with' : null;
                        if (_cmp_) {
                            query[q] = query[q].replace('^\\Q', '');
                            query[q] = query[q].replace('\\E', '');
                            exp = this.createExp(key, query[q], _cmp_, not);
                            this.KeyConditionExpression = this.KeyConditionExpression.replace('[first]', exp);
                        } else {
                            throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : Cannot apply [contains] on objectId');
                        }
                        break;
                    case '_id':
                        if (query[q].constructor === Object && this.isQuery(query[q])) {
                            this.buildKC(query[q], q, not, _op);
                        } else {
                            exp = this.createExp(q, query[q], '=', not);
                            this.KeyConditionExpression = this.KeyConditionExpression.replace('[first]', exp);
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

    _getProjectionExpression(keys : string[], _params) {
        if (!_params.ExpressionAttributeNames) {
            _params.ExpressionAttributeNames = {};
        }
        let attributes = Object.keys(_params.ExpressionAttributeNames);
        keys = keys.map(
            key => {
                let _key = '#' + key.replace(/^(_|\$)+/, '');
                if (!(_key in attributes)) {
                    _params.ExpressionAttributeNames[_key] = key;
                }
                return _key;
            }
        )

        return keys.join(', ');
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
            params.ProjectionExpression = this._getProjectionExpression(keys, params);
        }

        return new Promise(
            (resolve, reject) => {
                this.dynamo.get(params, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        if (data.Item) {
                            delete data.Item._pk_className;
                        }
                        if (data.Item) {
                            resolve([data.Item]);
                        } else {
                            resolve([]);
                        }
                    }
                })
            }
        );
    }

    _query(query: Object = {}, options : Options = {}) : Promise {
        
        const between = (n, a, b) => {
            return (n - a) * (n - b) <= 0
        }

        return new Promise(
            (resolve, reject) => {

                const _exec = (query: Object = {}, options : Options = {}, params : DynamoDB.DocumentClient.QueryInput = null, results = []) => {
             
                    if (!params) {
                        let keys = Object.keys(options.keys || {});
                        let count = options.count ? true : false;

                        // maximum by DynamoDB is 100 or 1MB
                        let limit;
                        if (!count) {
                            limit = options.limit ? options.limit : 100;
                        }

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
                            Select : select
                        }

                        if (!count) {
                            _params.Limit = limit;
                        }

                        if (Object.keys(query).length > 0) {
                        
                            let exp : FilterExpression = new FilterExpression();
                            let _query1 = JSON.parse(JSON.stringify(query));
                            let _query2 = JSON.parse(JSON.stringify(query));
                            exp = exp.build(_query1);
                            _params.FilterExpression = exp.FilterExpression;
                            _params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
                            _params.ExpressionAttributeValues = exp.ExpressionAttributeValues;

                            exp = exp.buildKC(_query2);
                            if (!exp.KeyConditionExpression.startsWith('[first]')) {
                                _params.KeyConditionExpression = '#className = :className AND ( [next] )';
                                _params.KeyConditionExpression = _params.KeyConditionExpression.replace('[next]', exp.KeyConditionExpression);
                                _params.ExpressionAttributeNames['#className'] = '_pk_className';
                                _params.ExpressionAttributeValues[':className'] = this.className;
                                delete _params.KeyConditions;
                            }
                        }

                        if (descending) {
                            _params.ScanIndexForward = descending;
                        }

                        if (keys.length > 0) {
                            _params.ProjectionExpression = this._getProjectionExpression(keys, _params);
                        }

                        params = _params;
                    }

                    this.dynamo.query(params, (err, data) => {
                        if (err) {
                            reject(err);
                        } else {
                            results = results.concat(data.Items || []);
                            if (data.LastEvaluatedKey && (results.length < options.limit)) {
                                options.limit = options.limit - results.length;
                                params.ExclusiveStartKey = data.LastEvaluatedKey;
                                return _exec(query, options, params, results);
                            }

                            if (options.count) {
                                resolve(data.Count ? data.Count : 0);
                            }

                            results.forEach((item) => {
                                delete item._pk_className;
                            });

                            resolve(results);
                        }
                    });
                }

                _exec(query, options);
            }

        )
    }

    find(query: Object = {}, options : Options = {}) : Promise {
        if (query.hasOwnProperty('_id') && typeof query['_id'] === 'string') {
            let id = query['_id'];
            let _keys = options.keys || {};
            //delete _keys['_id'];
            let keys = Object.keys(_keys);
            return this._get(id, keys);
        } else {
            //if (options.keys && options.keys['_id']) {
            //    delete options.keys['_id'];
            //}
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
        let id = query['_id'] || object['_id'];
        
        let params : DynamoDB.DocumentClient.UpdateItemInput = {
            TableName : this.database,
            Key: {
                _pk_className : this.className
            },
            ReturnValues : 'ALL_NEW'
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
            } else {
                throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : you must specify query keys');
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

        return new Promise(
            (resolve, reject) => {
                this.dynamo.update(params, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        if (data && data.Attributes) {
                            delete data.Attributes._pk_className;
                        }
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