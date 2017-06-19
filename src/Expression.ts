import { Parse } from 'parse/node';
import { $ } from './helpers';
import { DynamoDB } from 'aws-sdk';

var u = require('util'); // for debugging;

// helper class to generate DynamoDB FilterExpression from MongoDB Query Object
export class Expression {

    Expression : string = '[first]';
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

    // legacy util, use Expressions intead
    static createCondition (comperator : string, value) : DynamoDB.DocumentClient.Condition {

        const DynamoComperator = {
            '$gt': 'GT',
            '$lt': 'LT',
            '$gte': 'GE',
            '$lte': 'LE',
            '$eq' : 'EQ',
            '$ne' : 'NE',
            '$in' : 'IN',
            '$exists' : 'NOT_NULL',
            '$regex' : 'BEGINS_WITH'
        }
        
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

    static getDynamoQueryFilter(query : Object = {}) : DynamoDB.DocumentClient.FilterConditionMap {
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

    static getProjectionExpression(keys : string[], _params) : string {
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
                const _p = Expression.transformPath(_params, key);
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

    static getUpdateExpression(object : Object, _params) : string {

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
                        _params.ExpressionAttributeValues[':__zero__'] = 0;
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

        Object.keys($set).forEach(key => {
            if ($set[key] !== undefined) {
                let keys = Expression.transformPath(_params, key, $set[key]);
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
                let keys = Expression.transformPath(_params, key, $inc[key]);
                let exp = '[key] = if_not_exists([key],:__zero__) + [value]';
                exp = exp.replace(/\[key\]/g, keys);
                exp = exp.replace('[value]', _params._v);
                _set.push(exp);
            }
        });

        _unset = _unset.concat(Object.keys($unset).map(
            key => Expression.transformPath(_params, key)
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

    createExpression(key : string, value : any, op, not = false, _all = false) : string {
        
        if (!op) {
            throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : Operation is not supported');
        }

        let exp : string = '';
        let _key : string = Expression.transformPath(this, key, value);
        let _vl = this._v;

        switch (op) {
            case 'begins_with':
                exp = 'begins_with([key],[value])';
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
                exp = 'contains([key],[value])';   
                break;
            case 'IN':
                let _v = this.ExpressionAttributeValues[_vl].sort();
                if (_v.indexOf(null) > -1 || _v.indexOf(undefined) > -1) {
                    if (_v.length == 2) {
                        this.ExpressionAttributeValues[_vl] = _v[0];
                        this.ExpressionAttributeValues[':null'] = null;
                        exp = '( contains([key],[value]) OR attribute_not_exists([key]) OR [key] = :null )';
                    } else {
                        _v = _v.filter(e => e != null);
                        let _vs = _v.map(
                            (e,i) => {
                                let _k = _vl + '_' + i;
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
                    _v = _v.filter(e => e != null);
                    let _vs = _v.map(
                        (e,i) => {
                            let _k = _vl + '_' + i;
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
    static transformPath(params : any, path : string, value : any = undefined) : string {

        if (!path) {
            throw new Error('Key cannot be empty');
        }

        params = params || {};

        if (!params.hasOwnProperty('ExpressionAttributeNames')) {
            params.ExpressionAttributeNames = {}
        }

        if ((!params.hasOwnProperty('ExpressionAttributeNames')) && value) {
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
                // make sure key names don't overlap with each other
                if ($.getKey(params.ExpressionAttributeNames, keys[i].replace(/\[[0-9]+\]/g, '')) === null) {
                    let index = $.count(params.ExpressionAttributeNames, '#' + _k);
                    if (index > 0) {
                        _k = _k + '_' + index;
                    }
                    params.ExpressionAttributeNames['#' + _k] = keys[i].replace(/\[[0-9]+\]/g, '');
                }

                if (value !== undefined) {
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

    build(query = {}, key = null, not = false, _op = null) : Expression {
        let exp, _cmp_, size = 0;
        Object.keys(query).forEach(
            (q,i) => {

                if (i < Object.keys(query).length - 1 && Object.keys(query).length > 1) {
                    if (_op) {
                        this.Expression = this.Expression.replace('[first]','( [first] AND [next] )');
                    } else {
                        this.Expression = this.Expression.replace('[first]','[first] AND [next]');
                    }
                }

                if (i < Object.keys(query).length) {
                     this.Expression = this.Expression.replace('[next]', '[first]');
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
                                        this.Expression = this.Expression.replace('[first]','( [first] ' + this.comperators[q] + ' [next] )');
                                    } else {
                                        this.Expression = this.Expression.replace('[first]','[first] ' + this.comperators[q] + ' [next]');
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
                        exp = this.createExpression(key, query[q], _cmp_, false);
                        this.Expression = this.Expression.replace('[first]', exp);
                        break;
                    case '$in':
                    case '$nin':
                        let _not_;
                        if (q == '$nin' && not === true) _not_ = false;
                        if (q == '$nin' && not === false) _not_ = true;
                        if (q == '$in' && not === true) _not_ = true;
                        if (q == '$in' && not === false) _not_ = false;
                        query[q] = query[q] || [];
                        size = query[q].length;
                        if (size === 0) query[q] = ['*']; //throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : [$in] cannot be empty');
                        if (size === 1) query[q] = query[q][0];
                        if (size > 100) query[q] = query[q].slice(0,99); //throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : The [$in] operator is provided with too many operands, ' + size);
                        _cmp_ = size === 1 ? '=' : 'IN';
                        exp = this.createExpression(key, query[q], _cmp_, _not_);
                        this.Expression = this.Expression.replace('[first]', exp);
                        break;
                    case '$regex':
                        _cmp_ = query[q].startsWith('^') ? 'begins_with' : 'contains';
                        query[q] = query[q].replace('^', '');
                        query[q] = query[q].replace('\\Q', '');
                        query[q] = query[q].replace('\\E', '');
                        exp = this.createExpression(key, query[q], _cmp_, not);
                        this.Expression = this.Expression.replace('[first]', exp);
                        break;
                    case '$exists':
                        _cmp_ = query[q] ? 'attribute_exists' : 'attribute_not_exists';
                        exp = this.createExpression(key, query[q], _cmp_, not);
                        this.Expression = this.Expression.replace('[first]', exp);
                        break;
                    case '$not':
                        this.build(query[q], key, true, _op);
                        break;
                    case '$all':
                        query[q] = (query[q].constructor === Array ? query[q] : []).sort();
                        exp = this.createExpression(key, query[q], 'IN', not, true);
                        this.Expression = this.Expression.replace('[first]', exp);
                        break;
                    default:
                        if (query[q] && query[q].constructor === Object && this.isQuery(query[q])) {
                            this.build(query[q], q, not, _op);
                        } else {
                            if (query[q] === undefined) {
                                exp = this.createExpression(q, query[q], 'attribute_not_exists', not);
                            } else if (query[q] === null) {
                                exp = this.createExpression(q, query[q], '=', not);
                            } else {
                                exp = this.createExpression(q, query[q], '=', not);
                            }
                            this.Expression = this.Expression.replace('[first]', exp);
                        }
                        break;
                }
            }
        )

        return this;
    }
}