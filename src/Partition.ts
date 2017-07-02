// Partition is the class where objects are stored
// Partition is like Collection in MongoDB
// a Partition is represented by a hash or partition key in DynamoDB
import { DynamoDB } from 'aws-sdk';
import * as Promise from 'bluebird';
import { Parse } from 'parse/node';
import { newObjectId } from 'parse-server/lib/cryptoUtils';
import { Expression } from './Expression';
import { _Cache as Cache } from './Cache';
import { $ } from './helpers';
import { _ } from 'lodash';

var u = require('util'); // for debugging;

type Options = {
    skip? : Object, // not supported
    limit? : number,
    sort? : Object, // only supported on partition/sort key
    keys? : Object
    count?: boolean
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
            params.ProjectionExpression = Expression.getProjectionExpression(keys, params);
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
                        
                            let exp : Expression = new Expression();
                            exp = exp.build(query);
                            if (exp.Expression != '[first]') {
                                _params.FilterExpression = exp.Expression;
                                _params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
                                _params.ExpressionAttributeValues = exp.ExpressionAttributeValues;
                                _params.ExpressionAttributeNames['#className'] = '_pk_className';
                                _params.ExpressionAttributeValues[':className'] = this.className;
                            }
                        }

                        if (descending) {
                            _params.ScanIndexForward = descending;
                        }

                        if (keys.length > 0) {
                            _params.ProjectionExpression = Expression.getProjectionExpression(keys, _params);
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

                            if (results.length > 1 && Object.keys(options.sort).length > 0) {
                                results = _.orderBy(
                                    results,
                                    Object.keys(options.sort),
                                    $.values(options.sort).map((k) => { if (k == 1) return 'asc'; else return 'desc' })
                                );
                            }
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
        let id = query['_id'];
        if (id && typeof id === 'string' && !(query.hasOwnProperty('_rperm') || query.hasOwnProperty('acl'))) {
            let _keys = options.keys || {};
            let keys = Object.keys(_keys);
            return this._get(id, keys);
        }
        return this._query(query, options);
    }

    count(query: Object = {}, options : Options = {}) : Promise {
        options.count = true;
        return this._query(query, options);
    }

    ensureUniqueness(object) : Promise {
        if (Object.keys(object || {}).length > 0 && Cache.get('UNIQUE')) {
            return Cache.get('UNIQUE', this.className).then(value => {
                if (value) {
                    return Promise.resolve(value);
                } else {
                    return this.relaodUniques();
                }
            }).then(uniques => {
                if (uniques.length > 0) {
                    uniques = _.intersection(uniques, Object.keys(object));
                    if (uniques.length > 0) {
                        return this.count({
                            $or : uniques.map(key => {
                                return { [key] : object[key] }
                            })
                        });
                    } else {
                        return Promise.resolve(0);
                    }
                } else {
                    return Promise.resolve(0);
                }
            }).catch(error => {
                throw error
            });
        } else {
            return Promise.resolve(0);
        }
    }

    relaodUniques() {
        let params : DynamoDB.DocumentClient.GetItemInput = {
            TableName : this.database,
            Key: {
                _pk_className : '_UNIQUE_INDEX_',
                _sk_id : this.className
            },
            ConsistentRead : true
        }

        return this.dynamo.get(params).promise().then(
            item => {
                let uniques = [];
                if (item && item['fields']) {
                    Cache.put('UNIQUE', { [this.className] : item['fields'] });
                    uniques = item['fields'];
                } else {
                    Cache.put('UNIQUE', { [this.className] : [] });
                }

                return Promise.resolve(uniques);
            }
        );
    }

    insertOne(object) : Promise {
        let id = object['_id'] || newObjectId();
        object['_id'] = id;

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
                        resolve({ ok : 1, n : 1, ops : [ object ], insertedId : id });
                    }
                });
            }
        )
    }

    // update in DynamoDB is upsert by default but if used with ConditionExpression, it will fail
    // if upsert is true, try to find the item 
    updateOne(query = {}, object : Object, upsert = false) : Promise {
        //console.log('UPDATE', query, object);
        let id = query['_id'];
        
        let params : DynamoDB.DocumentClient.UpdateItemInput = {
            TableName : this.database,
            Key: {
                _pk_className : this.className
            },
            ReturnValues : 'ALL_NEW'
        }

        let find = Promise.resolve();

        if (id) {
            if (typeof id == 'string') {
                find = this._get(id).then(
                    result => {
                        if (result.length > 0 && result[0]._id === id) {
                            return result[0];
                        }
                        return null;
                    }
                );
            } else {
                return this.updateMany(query, object, upsert);
            }
        } else {
            if (Object.keys(query).length > 0) {
                find = this.find(query, { limit : 1 }).then(
                    results => {
                        if (results.length > 0 && results[0]._id) {
                            return results[0];
                        }
                        return null;
                    }
                )
            } else {
                throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : you must specify query keys');
            }
        }
        
        let exp : Expression = new Expression();
        exp = exp.build(query);
        params.ConditionExpression = exp.Expression;
        params.ExpressionAttributeNames = exp.ExpressionAttributeNames;
        params.ExpressionAttributeValues = exp.ExpressionAttributeValues;

        return new Promise(
            (resolve, reject) => {
                find.then((result) => {
                    if (result && result._id) {
                        params.UpdateExpression = Expression.getUpdateExpression(object, params, result);
                        params.Key._sk_id = result._id;
                        //console.log('UPDATE PARAMS', params);
                        this.dynamo.update(params, (err, data) => {
                            if (err) {
                                if (err.name == 'ConditionalCheckFailedException') {
                                    if (upsert) {
                                        reject(err);
                                    } else {
                                        resolve({ ok : 1, n : 0, nModified : 0, value : null});
                                    }
                                } else {
                                    reject(err);
                                }
                            } else {
                                if (data && data.Attributes) {
                                    data.Attributes._id = data.Attributes._sk_id;
                                    delete data.Attributes._pk_className;
                                    delete data.Attributes._sk_id;
                                    resolve({ ok : 1, n : 1, nModified : 1, value : data.Attributes });
                                } else {
                                    resolve({ ok : 1, n : 1, nModified : 1, value : null });
                                }
                            }
                        });
                    } else {
                        // here we do upserting
                        if (upsert) {
                            object = {
                                ...object['$set'],
                                ...object['$inc']
                            }
                            object['_id'] = newObjectId();
                            this.insertOne(object).then(
                                res => resolve({ ok : 1, n : 1, nModified : 1, value : res.ops[0] })
                            );
                        } else {
                            resolve({ ok : 1, n : 1, nModified : 1, value : null });
                        }
                    }   
                });
            }
        )
    }

    upsertOne(query = {}, object : Object) : Promise {
        return this.updateOne(query, object, true);
    }

    updateMany(query = {}, object, upsert = false) : Promise {
        let id = query['_id'];

        if (typeof id == 'string') {
            return this.updateOne(query, object);
        } else {
            let options = {
                keys : { _id : 1 }
            }

            return this.find(query, options).then(
                (res) => {
                    res = res.filter(item => item._id != undefined);
                    if (res.length === 0) throw new Parse.Error(Parse.Error.INVALID_QUERY, 'DynamoDB : cannot update nothing');

                    let promises = res.map(
                        item => this.updateOne({ _id : item._id }, object, upsert)
                    );

                    return new Promise(
                        (resolve, reject) => {
                            Promise.all(promises).then(
                                res => {
                                    res = res.filter(item => item.value != undefined);
                                    if (res.length > 0) {
                                        resolve(res);
                                    } else {
                                        resolve(null);
                                    }
                                }
                            ).catch(
                                err => reject(err)
                            );
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

        let exp : Expression = new Expression();
        exp = exp.build(query);
        params.ConditionExpression = exp.Expression;
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
                                if (err.name == 'ConditionalCheckFailedException') {
                                    resolve({ ok : 1, n : 0, deletedCount : 0 });
                                } else {
                                    reject(err);
                                }
                            } else {
                                resolve({ ok : 1, n : 1, deletedCount : 1 });
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
                    if (res.length === 0) throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found');

                    let promises = res.map(
                        item => this.deleteOne({ _id : item._id })
                    );
                    
                    return new Promise(
                        (resolve, reject) => {
                            Promise.all(promises).then(
                                res => resolve({ ok : 1, n : res.length, deletedCount : res.length })
                            ).catch(
                                () => { throw new Parse.Error(Parse.Error.INTERNAL_SERVER_ERROR, 'DynamoDB : Internal Error'); }
                            );
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