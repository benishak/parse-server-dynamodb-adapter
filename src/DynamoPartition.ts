// Partition is the class where objects are stored
// Partition is like Collection in MongoDB
// a Partition is represented by a hash or partition key in DynamoDB

import { DynamoDB } from 'aws-sdk';
import * as Promise from 'bluebird';
import { Parse } from 'parse/node';
import { newObjectId } from 'parse-server/lib/cryptoUtils';
import { generate as genRandomString } from 'randomstring';
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

// Object.values ES7
class $ extends Object {
    static values (obj : Object) {
        return Object.keys(obj).map(e => obj[e]);
    }

    static count (obj : Object, s : string) {
        return Object.keys(obj).filter(e => { if (e.indexOf(s) === 0) return e }).length;
    }

    static getKey(obj : Object, v : any) {
        for (let k in obj) {
            if (obj[k] === v) return k;
        }

        return null;
    }
}

// helper class to generate DynamoDB FilterExpression from MongoDB Query Object
export class FilterExpression {

    FilterExpression : string = '[first]';
    KeyConditionExpression : string = '[first]';
    ExpressionAttributeValues = {};
    ExpressionAttributeNames = {};
    private _v : string;

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
        '$all' : '*'
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

    createExp(key : string, value : any, op, not = false, _all = false) : string {
        
        if (!op) {
            throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : Operation is not supported');
        }

        let exp : string = '';
        let _key : string = FilterExpression._transformPath(this, key, value);
        let _vl = this._v;

        switch (op) {
            case 'begins_with':
                exp = 'begins_with([key], [value])';
                break;
            case 'attribute_exists':
                exp = 'attribute_exists([key])';
                delete this.ExpressionAttributeValues[_vl];
                break;
            case 'attribute_not_exists':
                exp = 'attribute_not_exists([key])';
                delete this.ExpressionAttributeValues[_vl];
                break;
            case 'contains':
                exp = 'contains([key], [value])';   
                break;
            case 'IN':
                let _v = this.ExpressionAttributeValues[_vl].sort();
                //let _k = ':' + _vl + '_' + index;
                if (_v.indexOf(null) > -1 || _v.indexOf(undefined) > -1) {
                    if (_v.length == 2) {
                        this.ExpressionAttributeValues[_vl] = _v[0];
                        this.ExpressionAttributeValues[':null'] = null;
                        exp = '( contains([key],[value]) OR attribute_not_exists([key]) OR [key] = :null )';
                    } else {
                        _v = _v.filter(e => e != null);
                        let _vs = _v.map(
                            (e,i) => {
                                let _k = _vl + '_' + i;//+ _kk + '_' + index + '_' + i;
                                let exp = 'contains([key],[value])';
                                this.ExpressionAttributeValues[_k] = e;
                                exp = exp.replace('[value]', _k);
                                return exp;
                            }
                        )
                        delete this.ExpressionAttributeValues[_vl];
                        this.ExpressionAttributeValues[':null'] = null;
                        exp = '( [exp] OR attribute_not_exists([key]) OR [key] = :null )'.replace('[exp]', _vs.join(' OR '));
                    }
                } else {
                    //let _k = ':' + _kk + '_' + index;
                    _v = _v.filter(e => e != null);
                    //this.ExpressionAttributeValues[_k] = _v;
                    let _vs = _v.map(
                        (e,i) => {
                            let _k = _vl + '_' + i; //':' + _kk + '_' + index + '_' + i;
                            this.ExpressionAttributeValues[_k] = e;
                            if (_all) {
                                let exp = 'contains([key],[value])';
                                exp = exp.replace('[value]', _k);
                                return exp;
                            }
                            return _k;
                        }
                    )
                    delete this.ExpressionAttributeValues[_vl];
                    //delete this.ExpressionAttributeValues[_k];
                    if (_all) {
                        exp = '( [exp] )'.replace('[exp]', _vs.join(' AND '));
                    } else {
                        exp = '[key] IN ([value])'.replace('[value]', _vs.join());
                    }
                }
                break;
            default:
                exp = '[key] [op] [value]';
                break;
        }

        exp = exp.replace(/\[key\]/g, _key);
        exp = exp.replace(/\[value\]/g, _vl);
        exp = exp.replace(/\[op\]/g, op);

        if (not) {
            exp = 'NOT ( ' + exp + ' )';
        }

        return exp;
    }

    // set ExpressionAttributeNames and ExpressionAttributeValues
    // and returns the transformed path
    // e.g. _id -> #id
    // e.g. item.users[1].id -> #item.#users[1].#id
    static _transformPath(params : any, path : string, value : any = null) : string {

        if (!path) {
            throw new Error('Key cannot be empty');
        }

        params = params || {};

        if (!params.hasOwnProperty('ExpressionAttributeNames')) {
            params.ExpressionAttributeNames = {}
        }

        if (value && !params.hasOwnProperty('ExpressionAttributeNames')) {
            params.ExpressionAttributeValues = {}
        }

        let _key = path.replace(/^(_|\$)+/, '');
        let index = 0, _vl : string;
        let keys = path.split('.');

        let attributes = Object.keys(params.ExpressionAttributeNames);
        for (let i=0; i < keys.length; i++) {
            let _key = keys[i].replace(/^(_|\$)+/, '');
            _key = _key.toLowerCase();
            let _k = _key.replace(/\[[0-9]+\]/g, '');
            if (attributes.indexOf(_key) == -1) {
                // make sure key names doesn't overlap with each other
                if ($.getKey(params.ExpressionAttributeNames, keys[i].replace(/\[[0-9]+\]/g, '')) === null) {
                    let index = $.count(params.ExpressionAttributeNames, '#' + _k);
                    if (index > 0) {
                        _k = _k + '_' + index;
                    }
                    params.ExpressionAttributeNames['#' + _k] = keys[i].replace(/\[[0-9]+\]/g, '');
                }

                if (value != null) {
                    if (i == (keys.length - 1)) {
                        let index = $.count(params.ExpressionAttributeValues, ':' + _k);
                        _vl = ':' + _k + '_' + index;
                        params.ExpressionAttributeValues[_vl] = value;
                    }
                }
            }
            keys[i] = keys[i].replace(keys[i].replace(/\[[0-9]+\]/g, ''), $.getKey(params.ExpressionAttributeNames, keys[i].replace(/\[[0-9]+\]/g, '')));
        }

        params._v = _vl;

        return keys.join('.');
    }

    build(query = {}, key = null, not = false, _op = null) : FilterExpression {
        let exp, _cmp_, size = 0;
        Object.keys(query).forEach(
            (q,i) => {

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
                        query[q] = query[q].filter(sq => {
                            if (sq && sq.constructor === Object && Object.keys(sq).length > 0) {
                                return sq;
                            }
                        });
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
                        not = q == '$nin' ? true : not;
                        query[q] = query[q] || [];
                        size = query[q].length;
                        if (size === 0) query[q] = ['*']; //throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : [$in] cannot be empty');
                        if (size === 1) query[q] = query[q][0];
                        if (size > 100) query[q] = query[q].slice(0,99); //throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : The [$in] operator is provided with too many operands, ' + size);
                        _cmp_ = size === 1 ? '=' : 'IN';
                        exp = this.createExp(key, query[q], _cmp_, not);
                        this.FilterExpression = this.FilterExpression.replace('[first]', exp);
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
                    case '$all':
                        query[q] = (query[q].constructor === Array ? query[q] : []).sort();
                        exp = this.createExp(key, query[q], 'IN', not, true);
                        this.FilterExpression = this.FilterExpression.replace('[first]', exp);
                        break;
                    default:
                        if (query[q] && query[q].constructor === Object && this.isQuery(query[q])) {
                            this.build(query[q], q, not, _op);
                        } else {
                            if (query[q] === undefined) {
                                exp = this.createExp(q, query[q], 'attribute_not_exists', not);
                            } else if (query[q] === null) {
                                exp = this.createExp(q, query[q], '=', not);
                            } else {
                                exp = this.createExp(q, query[q], '=', not);
                            }
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

    buildUpdateExpression(query) {
        
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

    _getProjectionExpression(keys : string[], _params) : string {
        let projection : string[] = [];

        if (!_params.ExpressionAttributeNames) {
            _params.ExpressionAttributeNames = {};
        }

        _params.ExpressionAttributeNames['#id'] = '_id';
        _params.ExpressionAttributeNames['#sortId'] = '_sk_id';
        _params.ExpressionAttributeNames['#updated_at'] = '_updated_at';
        _params.ExpressionAttributeNames['#created_at'] = '_created_at';
        
        let attributes = Object.keys(_params.ExpressionAttributeNames);
        keys.forEach(key => {
            if (key) {
                // let path = key.split('.');
                // for (let i=0; i < path.length; i++) {
                //     let _key = '#' + path[i].replace(/^(_|\$)+/, '');
                //     _key = _key.replace(/\[[0-9]+\]/, '');
                //     _key = _key.toLowerCase();
                //     if (attributes.indexOf(_key) == -1) {
                //         if ($.getKey(_params.ExpressionAttributeNames, path[i].replace(/\[[0-9]+\]/g, '')) === null) {
                //             let index = $.count(_params.ExpressionAttributeNames, _key);
                //             if (index > 0) {
                //                 _key = _key + '_' + index;
                //             }
                //             _params.ExpressionAttributeNames[_key] = path[i].replace(/\[[0-9]+\]/g, '');
                //         }
                //         //_params.ExpressionAttributeNames[_key] = key;
                //     }
                //     //path[i] = _key;
                //     path[i] = path[i].replace(path[i].replace(/\[[0-9]+\]/g, ''), $.getKey(_params.ExpressionAttributeNames, path[i].replace(/\[[0-9]+\]/g, '')));
                // }
                const _p = FilterExpression._transformPath(_params, key);
                if (keys.length > 0 && projection.indexOf(_p) == -1) {
                    projection.push(_p);
                }
            }
        });

        if (projection.indexOf('#id') == -1) projection.push('#id');
        if (projection.indexOf('#sortId') == -1) projection.push('#sortId');
        if (projection.indexOf('#updated_at') == -1) projection.push('#updated_at');
        if (projection.indexOf('#created_at') == -1) projection.push('#created_at');
        return projection.sort().join(', ');
    }

    _getUpdateExpression(object : Object, _params) : string {

        if (!_params.ExpressionAttributeNames) {
            _params.ExpressionAttributeNames = {};
        }

        if (!_params.ExpressionAttributeValues) {
            _params.ExpressionAttributeValues = {};
        }

        let $set = {}, $unset = [], $inc = {};
        let _set = [], _unset = [], _inc = [];
        let exp;

        Object.keys(object || {}).forEach(
            _op => {
                switch (_op) {
                    case '$setOnInsert':
                    case '$set':
                        $set = object['$set'] || {};
                        delete $set['_id'];
                        delete $set['_sk_id'];
                        delete $set['_pk_className'];
                        break;
                    case '$unset':
                        $unset = object['$unset'] || {};
                        delete $unset['_id'];
                        delete $unset['_sk_id'];
                        delete $unset['_pk_className'];
                        break;
                    case '$inc':
                        $inc = object['$inc'] || {};
                        delete $inc['_id'];
                        delete $inc['_sk_id'];
                        delete $inc['_pk_className'];
                        break;
                    case '$mul':
                    case '$min':
                    case '$max':
                    case '$rename':
                    case '$currentDate':
                    case '$each':
                    case '$sort':
                    case '$slice':
                    case '$position':
                    case '$bit':
                    case '$isolated':
                        throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : [' + _op + '] not supported on update');
                    default:
                        $set = object;
                        break;
                }
            }
        );

        let attributes = Object.keys(_params.ExpressionAttributeNames || {});
        let attributes_v = $.values(_params.ExpressionAttributeNames || {});

        Object.keys($set).forEach(key => {
            if ($set[key] != undefined) {
                //const keys = key.split('.');
                // for (let i=0; i < keys.length; i++) {
                //     let _key = keys[i].replace(/^(_|\$)+/, '');
                //     _key = _key.toLowerCase();
                //     let _k = _key.replace(/\[[0-9]+\]/g, '');
                //     if (attributes.indexOf(_key) == -1) {
                //         if ($.getKey(_params.ExpressionAttributeNames, keys[i].replace(/\[[0-9]+\]/g, '')) === null) {
                //             let index = $.count(_params.ExpressionAttributeNames, '#' + _k);
                //             if (index > 0) {
                //                 _k = _k + '_' + index;
                //             }
                //             _params.ExpressionAttributeNames['#' + _k] = keys[i].replace(/\[[0-9]+\]/g, '');
                //         }
                //         if (i == (keys.length - 1)) {
                //             _params.ExpressionAttributeValues[':' + lv] = $set[key];
                //         }
                //     }
                //     keys[i] = keys[i].replace(keys[i].replace(/\[[0-9]+\]/g, ''), $.getKey(_params.ExpressionAttributeNames, keys[i].replace(/\[[0-9]+\]/g, '')))
                //                      .replace('#', '');
                // }
                let keys = FilterExpression._transformPath(_params, key, $set[key]);
                let exp = '[key] = [value]';
                exp = exp.replace('[key]', keys);
                exp = exp.replace('[value]', _params._v);
                _set.push(exp);
            } else {
                if ($unset.indexOf(key) == -1) {
                    _unset.push(key);
                }
            }
        });

        Object.keys($inc).forEach(key => {
            if ($inc[key] != undefined) {
                // const lv = genRandomString(5);
                // const keys = key.split('.');
                // for (let i=0; i < keys.length; i++) {
                //     let _key = keys[i].replace(/^(_|\$)+/, '');
                //     _key = _key.toLowerCase();
                //     let _k = _key.replace(/\[[0-9]+\]/g, '');
                //     if (attributes.indexOf(_key) == -1) {
                //         if ($.getKey(_params.ExpressionAttributeNames, keys[i].replace(/\[[0-9]+\]/g, '')) === null) {
                //             let index = $.count(_params.ExpressionAttributeNames, '#' + _k);
                //             if (index > 0) {
                //                 _k = _k + '_' + index;
                //             }
                //             _params.ExpressionAttributeNames['#' + _k] = keys[i].replace(/\[[0-9]+\]/g, '');
                //         }
                //         if (i == (keys.length - 1)) {
                //             _params.ExpressionAttributeValues[':' + lv] = $inc[key];
                //         }
                //     }
                //     //keys[i] = keys[i].replace(keys[i], _key);
                //     keys[i] = keys[i].replace(keys[i].replace(/\[[0-9]+\]/g, ''), $.getKey(_params.ExpressionAttributeNames, keys[i].replace(/\[[0-9]+\]/g, '')))
                //                      .replace('#', '');
                // }

                let keys = FilterExpression._transformPath(_params, key, $inc[key]);
                let exp = '[key] = [key] + [value]';
                exp = exp.replace(/\[key\]/g, keys);
                exp = exp.replace('[value]', _params._v);
                _set.push(exp);
            }
        });

        _unset = _unset.concat(Object.keys($unset).map(
            key => {
                // const keys = key.split('.');
                // for (let i=0; i < keys.length; i++) {
                //     let _key = '#' + keys[i].replace(/^(_|\$)+/, '');
                //     _key = _key.replace(/\[[0-9]+\]/g, '');
                //     if (attributes.indexOf(_key) == -1) {
                //         if ($.getKey(_params.ExpressionAttributeNames, keys[i].replace(/\[[0-9]+\]/g, '')) === null) {
                //             let index = $.count(_params.ExpressionAttributeNames, _key);
                //             if (index > 0) {
                //                 _key = _key + '_' + index;
                //             }
                //             _params.ExpressionAttributeNames[_key] = keys[i].replace(/\[[0-9]+\]/g, '');
                //         }
                //     }
                //     //keys[i] = keys[i].replace(keys[i], _key);
                //     keys[i] = keys[i].replace(keys[i].replace(/\[[0-9]+\]/g, ''), $.getKey(_params.ExpressionAttributeNames, keys[i].replace(/\[[0-9]+\]/g, '')))
                //                      .replace('#', '');
                // }
                return FilterExpression._transformPath(_params, key);
            }
        ));

        if (_set.length > 0) {
            if (exp) {
                exp = exp + ' SET ' + _set.join(', ');
            } else {
                exp = 'SET ' + _set.join(', ');
            }
        }

        if (_unset.length) {
            if (exp) {
                exp = exp + ' REMOVE ' + _unset.join(', ');
            } else {
                exp = 'REMOVE ' + _unset.join(', ');
            }
        }
        
        delete _params._v;
        //console.log('UPDATE EXPRESSION', exp);
        return exp;
    }

    _get(id : string, keys : string[] = []) : Promise {
        keys = keys || [];
        let params : DynamoDB.DocumentClient.GetItemInput = {
            TableName : this.database,
            Key: {
                _pk_className : this.className,
                _sk_id : id
            },
            ConsistentRead : true
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
                            data.Item._id = data.Item._sk_id;
                            delete data.Item._pk_className;
                            delete data.Item._sk_id;
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
        //if (Object.keys(query).length > 0) console.log('QUERY', this.className, u.inspect(query, false, null));
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
                            KeyConditionExpression : '#className = :className',
                            Select : select,
                            ExpressionAttributeNames : {
                                '#className' : '_pk_className'
                            },
                            ExpressionAttributeValues : {
                                ':className' : this.className
                            },
                            ConsistentRead : true
                        }

                        if (!count) {
                            _params.Limit = limit;
                        }

                        if (Object.keys(query).length > 0) {
                        
                            let exp : FilterExpression = new FilterExpression();
                            exp = exp.build(query);
                            _params.FilterExpression = exp.FilterExpression;
                            _params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
                            _params.ExpressionAttributeValues = exp.ExpressionAttributeValues;
                            _params.ExpressionAttributeNames['#className'] = '_pk_className';
                            _params.ExpressionAttributeValues[':className'] = this.className;
                        }

                        if (descending) {
                            _params.ScanIndexForward = descending;
                        }

                        if (keys.length > 0) {
                            _params.ProjectionExpression = this._getProjectionExpression(keys, _params);
                        }

                        params = _params;
                    }
                    //if (params.ProjectionExpression) console.log('QUERY EXP', this.className, params.ProjectionExpression, params.ExpressionAttributeNames);
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
                                item._id = item._sk_id;
                                delete item._pk_className;
                                delete item._sk_id;
                            });
                            //console.log('QUERY RESULT', this.className, u.inspect(results, false, null))
                            resolve(results);
                        }
                    });
                }

                _exec(query, options);
            }
        )
    }

    find(query: Object = {}, options : Options = {}) : Promise {
        if (query.hasOwnProperty('_id') && typeof query['_id'] === 'string' && !query.hasOwnProperty('_rperm')) {
            let id = query['_id'];
            let _keys = options.keys || {};
            //delete _keys['_id'];
            let keys = Object.keys(_keys);
            return this._get(id, keys);
        }

        // if (options.keys && options.keys['_id']) {
        //     delete options.keys['_id'];
        // }

        return this._query(query, options);
    }

    count(query: Object = {}, options : Options = {}) : Promise {
        options.count = true;
        return this._query(query, options);
    }

    insertOne(object) : Promise {
        let id = object['_id'] || newObjectId();

        let params : DynamoDB.DocumentClient.PutItemInput = {
            TableName : this.database,
            Item: {
                _pk_className : this.className,
                _sk_id : id,
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
        //console.log('UPDATE', query, object);
        let id = query['_id'] || object['_id'];
        
        let params : DynamoDB.DocumentClient.UpdateItemInput = {
            TableName : this.database,
            Key: {
                _pk_className : this.className
            },
            ReturnValues : 'ALL_NEW'
        }

        let find = Promise.resolve();

        if (id) {
            find = Promise.resolve(id);
        } else {
            if (Object.keys(query).length > 0) {
                find = this.find(query, { limit : 1 }).then(
                    results => {
                        if (results.length > 0 && results[0]._id) {
                            return results[0]._id;
                        } else {
                            // create new object if not exist
                            return newObjectId();
                        }
                    }
                )
            } else {
                throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : you must specify query keys');
            }
        }

        
        let exp = new FilterExpression();
        exp = exp.build(query);
        params.ConditionExpression = exp.FilterExpression;
        params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
        params.ExpressionAttributeValues = exp.ExpressionAttributeValues;

        params.UpdateExpression = this._getUpdateExpression(object, params);
        object = null; // destroy object;

        return new Promise(
            (resolve, reject) => {
                find.then((id) => {
                    params.Key._sk_id = id;
                    //console.log('UPDATE PARAMS', params);
                    this.dynamo.update(params, (err, data) => {
                        if (err) {
                            if (err.name == 'ConditionalCheckFailedException') reject(new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found'));
                            else reject(err);
                        } else {
                            if (data && data.Attributes) {
                                data.Attributes._id = data.Attributes._sk_id;
                                delete data.Attributes._pk_className;
                                delete data.Attributes._sk_id;
                            }
                            resolve({
                                ok : 1,
                                n : 1,
                                nModified : 1,
                                value : data.Attributes
                            });
                        }
                    });
                });
            }
        )
    }

    upsertOne(query = {}, object : Object) : Promise {
        return this.updateOne(query, object);
    }

    updateMany(query = {}, object) : Promise {
        let id = query['_id'] || object['_id'];

        if (id) {
            return this.updateOne(query, object);
        } else {
            let options = {
                keys : { _id : 1 }
            }

            return this.find(query, options).then(
                (res) => {
                    res = res.filter(item => item._id != undefined);
                    if (res.length === 0) throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : cannot delete nothing');

                    let promises = [];

                    let params : DynamoDB.DocumentClient.BatchWriteItemInput = {
                        RequestItems : {},
                    }

                    params.RequestItems[this.database] = res.map(item => {
                        return {
                            PutRequest : {
                                Item : {
                                    _pk_className : this.className,
                                    _sk_id : item._id,
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

        let find = Promise.resolve();

        if (id) {
            find = Promise.resolve(id);
        } else {
            if (Object.keys(query).length > 0) {
                find = this.find(query, { limit : 1 }).then(
                    results => {
                        if (results.length > 0 && results[0]._id) {
                            return results[0]._id;
                        } else {
                            return -1;
                        }
                    }
                )
            } else {
                throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : you must specify query keys');
            }
        }

        let exp = new FilterExpression();
        exp = exp.build(query);
        params.ConditionExpression = exp.FilterExpression;
        params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
        params.ExpressionAttributeValues = exp.ExpressionAttributeValues;

        return new Promise(
            (resolve, reject) => {
                find.then((id) => {
                    if (id == -1) {
                        reject();
                    } else {
                        params.Key._sk_id = id;
                        this.dynamo.delete(params, (err, data) => {
                            if (err) {
                                if (err.name == 'ConditionalCheckFailedException') reject(new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found'));
                                else reject(err);
                            } else {
                                resolve({
                                    ok : 1 ,
                                    n : 1,
                                    deletedCount : 1
                                })
                            }
                        });
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
                    res = res.filter(item => item._id != undefined);
                    if (res.length === 0) throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : cannot delete nothing');

                    let params : DynamoDB.DocumentClient.BatchWriteItemInput = {
                        RequestItems : {}
                    }

                    params.RequestItems[this.database] = res.map(item => {
                        return {
                            DeleteRequest : {
                                Key : {
                                    _pk_className : this.className,
                                    _sk_id : item._id
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

    _ensureSparseUniqueIndexInBackground(indexRequest) : Promise {
        return Promise.resolve();
    }

    drop () : Promise {
        return Promise.resolve();
    }
}