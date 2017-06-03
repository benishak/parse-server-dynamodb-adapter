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

    constructor(database : string, className : string) {
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

        return this.dynamo.get(params).promise();
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
                    className : this.createCondition('$eq', this.className),
                    _id : this.createCondition('$eq', this.className)
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

        return this.dynamo.query(params).promise().then(
            data => {
                results = results.concat(data.Items || []);
                if (data.LastEvaluatedKey && results.length < 100) {
                    options.limit = 100 - results.length;
                    params.ExclusiveStartKey = data.LastEvaluatedKey;
                    return this._query(query, options, params, results);
                }

                if (options.count) {
                    return Promise.resolve(data.Count ? data.Count : 0);
                }

                return Promise.resolve(results);
            }
        );
    }

    find(query: Object = {}, options : Options = {}) : Promise {
        return new Promise(
            (resolve, reject) => {

                let response : Promise;
                
                if (query.hasOwnProperty('_id')) {

                    response = this._get(
                        query['_id'],
                        Object.keys(options.keys || {})
                    );
                    
                } else {
                    
                    response = this._query(
                        query,
                        options
                    );
                }

                response.then(
                    data => {
                        resolve(data.Item || data.Items);
                    }
                ).catch(
                    error => {
                        reject(error);
                    }
                )
            }
        )
    }

    count(query: Object = {}, options : Options = {}) : Promise {
        options.count = true;
        return this._query(query, options);
    }
}