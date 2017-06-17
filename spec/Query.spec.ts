/// <reference path="../node_modules/mocha-typescript/globals.d.ts" />
import { suite, test, slow, timeout } from 'mocha-typescript';
import { should, expect, assert } from 'chai';
import { Partition } from '../src/Partition';
import { Expression as Query } from '../src/Expression';

const AWS = require('aws-sdk-mock');

const __ops0 = ['$eq', '$gt', '$gte', '$lt', '$lt', '$lte'];
const __ops1 = ['$eq', '$ne', '$gt', '$gte', '$lt', '$lt', '$lte'];
const __ops2 = ['$exists'];

@suite class DDBQuery {

    @test 'can generate simple expression from key : foo'() {
        let exp = new Query();
        let text = exp.createExpression('foo', 'bar', '=');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#foo');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':foo_0');
        expect(exp.ExpressionAttributeNames['#foo']).to.be.equal('foo');
        expect(exp.ExpressionAttributeValues[':foo_0']).to.be.equal('bar');
        expect(text).to.be.equal('#foo = :foo_0');
    }

    @test 'can generate expression from nested key : foo.bar'() {
        let exp = new Query();
        let text = exp.createExpression('foo.bar', 'foobar', '=');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#foo');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#bar');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':bar_0');
        expect(exp.ExpressionAttributeNames['#foo']).to.be.equal('foo');
        expect(exp.ExpressionAttributeNames['#bar']).to.be.equal('bar');
        expect(exp.ExpressionAttributeValues[':bar_0']).to.be.equal('foobar');
        expect(text).to.be.equal('#foo.#bar = :bar_0');
    }

    @test 'can generate expression from nested key : foo.bar.foobar'() {
        let exp = new Query();
        let text = exp.createExpression('foo.bar.foobar', 'foobar', '=');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#foo');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#bar');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':foobar_0');
        expect(exp.ExpressionAttributeNames['#foo']).to.be.equal('foo');
        expect(exp.ExpressionAttributeNames['#bar']).to.be.equal('bar');
        expect(exp.ExpressionAttributeValues[':foobar_0']).to.be.equal('foobar');
        expect(text).to.be.equal('#foo.#bar.#foobar = :foobar_0');
    }

    @test 'can generate expression from list element : foo[0]'() {
        let exp = new Query();
        let text = exp.createExpression('foo[0]', 'bar', '=');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#foo');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':foo_0');
        expect(exp.ExpressionAttributeNames['#foo']).to.be.equal('foo');
        expect(exp.ExpressionAttributeValues[':foo_0']).to.be.equal('bar');
        expect(text).to.be.equal('#foo[0] = :foo_0');
    }

    @test 'can generate expression from list element : foo[0][1]'() {
        let exp = new Query();
        let text = exp.createExpression('foo[0][1]', 'bar', '=');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#foo');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':foo_0');
        expect(exp.ExpressionAttributeNames['#foo']).to.be.equal('foo');
        expect(exp.ExpressionAttributeValues[':foo_0']).to.be.equal('bar');
        expect(text).to.be.equal('#foo[0][1] = :foo_0');
    }

    @test 'can generate expression of nested list element : foo.bar[0]'() {
        let exp = new Query();
        let text = exp.createExpression('foo.bar[0]', 'foobar', '=');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#foo');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#bar');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':bar_0');
        expect(exp.ExpressionAttributeNames['#foo']).to.be.equal('foo');
        expect(exp.ExpressionAttributeNames['#bar']).to.be.equal('bar');
        expect(exp.ExpressionAttributeValues[':bar_0']).to.be.equal('foobar');
        expect(text).to.be.equal('#foo.#bar[0] = :bar_0');
    }

    @test 'can generate expression of nested list elements : foo[0].bar[0]'() {
        let exp = new Query();
        let text = exp.createExpression('foo[0].bar[0]', 'foobar', '=');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#foo');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#bar');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':bar_0');
        expect(exp.ExpressionAttributeNames['#foo']).to.be.equal('foo');
        expect(exp.ExpressionAttributeNames['#bar']).to.be.equal('bar');
        expect(exp.ExpressionAttributeValues[':bar_0']).to.be.equal('foobar');
        expect(text).to.be.equal('#foo[0].#bar[0] = :bar_0');
    }

    @test 'can generate expression of mixed nested list elements : foo[0][1].bar[0].foobar[2]'() {
        let exp = new Query();
        let text = exp.createExpression('_foo[0][1].__bar[0].foobar[2]', '123', '=');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#foo');
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#bar');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':foobar_0');
        expect(exp.ExpressionAttributeNames['#foo']).to.be.equal('_foo');
        expect(exp.ExpressionAttributeNames['#bar']).to.be.equal('__bar');
        expect(exp.ExpressionAttributeValues[':foobar_0']).to.be.equal('123');
        expect(text).to.be.equal('#foo[0][1].#bar[0].#foobar[2] = :foobar_0');
    }

    @test 'DynamoDB FilterExpression : should generate simple query with single key'() {
        let exp = new Query();
        exp.build({ 
            "string" : "abc"
        });

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.be.equal(1);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.be.equal(1);
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#string');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':string_0');
        expect(exp.ExpressionAttributeNames['#string']).to.be.equal('string');
        expect(exp.ExpressionAttributeValues[':string_0']).to.be.equal('abc');
        expect(exp.Expression).to.be.equal('#string = :string_0');
    }

    @test 'DynamoDB FilterExpression : should generate single query with single key'() {
        __ops1.forEach(
            op => {
                let exp = new Query();
                exp.build({ 
                    field : {
                        [op] : 1
                    }
                });

                expect(Object.keys(exp.ExpressionAttributeNames).length).to.be.equal(1);
                expect(Object.keys(exp.ExpressionAttributeValues).length).to.be.equal(1);
                expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#field');
                expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':field_0');
                expect(exp.ExpressionAttributeNames['#field']).to.be.equal('field');
                expect(exp.ExpressionAttributeValues[':field_0']).to.be.equal(1);
                expect(exp.Expression).to.be.equal(
                    '#field [op] :field_0'.replace('[op]', exp.comperators[op])
                )
            }
        )
    }

    @test 'DynamoDB FilterExpression : should generate range query of signle key'() {
        let exp = new Query();
        exp.build({
            balance : {
                $gt : 1000,
                $lt : 2000
            }
        });

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.be.equal(1);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.be.equal(2);
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#balance');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':balance_0');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':balance_1');
        expect(exp.ExpressionAttributeNames['#balance']).to.be.equal('balance');
        expect(exp.ExpressionAttributeValues[':balance_0']).to.be.equal(1000);
        expect(exp.ExpressionAttributeValues[':balance_1']).to.be.equal(2000);
        expect((exp.Expression.match(/AND/g) || []).length).to.be.equal(1);
        expect(exp.Expression).to.be.equal(
            '#balance > :balance_0 AND #balance < :balance_1'
        );
    }

    @test 'DynamoDB FilterExpression : should generate range query of signle key with $and'() {
        let exp = new Query();
        exp.build({ $and : [
            { balance : { $gt : 1000 } },
            { balance : { $lt : 2000 } }
        ]});

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.be.equal(1);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.be.equal(2);
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#balance');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':balance_0');
        expect(exp.ExpressionAttributeValues).to.haveOwnProperty(':balance_1');
        expect(exp.ExpressionAttributeNames['#balance']).to.be.equal('balance');
        expect(exp.ExpressionAttributeValues[':balance_0']).to.be.equal(1000);
        expect(exp.ExpressionAttributeValues[':balance_1']).to.be.equal(2000);
        expect((exp.Expression.match(/AND/g) || []).length).to.be.equal(1);
        expect(exp.Expression).to.be.equal(
            '#balance > :balance_0 AND #balance < :balance_1'
        );
    }

    @test 'DynamoDB FilterExpression : should generate nested $or query of $ands'() {
        let exp = new Query();
        exp.build({ $and : [
            { 
                $or : [
                    { balance : { $gt : 1000 } },
                    { balance : { $lt : 2000 } }
                ]
            },
            {
                $or : [
                    { quantity : { $ne : 0 } },
                    { quantity : { $ne : 5000 } }
                ]
            }
        ]});

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.be.equal(2);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.be.equal(4);
        expect(exp.Expression).to.be.equal(
            '( #balance > :balance_0 OR #balance < :balance_1 ) AND ( #quantity <> :quantity_0 OR #quantity <> :quantity_1 )'
        );
    }

    @test 'DynamoDB FilterExpression : should generate $and query with multiple keys'() {
        let exp = new Query();
        exp.build({ $and : [
            { balance : { $gt : 1000 } },
            { balance : { $lt : 2000 } },
            { quantity : { $eq : 5 } },
            { product : 'book' }
        ]});

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.be.equal(3);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.be.equal(4);
        expect((exp.Expression.match(/AND/g) || []).length).to.be.equal(3);
        expect(exp.Expression).to.be.equal(
            '#balance > :balance_0 AND #balance < :balance_1 AND #quantity = :quantity_0 AND #product = :product_0'
        );
    }

    @test 'DynamoDB FilterExpression : should generate simple query with multiple keys without operator'() {
        let exp = new Query();
        exp.build({ 
            "string" : "string",
            "number" : 1,
            "date" : new Date().toISOString(),
            "double" : 1.5,
            "array" : [ 1, 2 ,3 ],
            "object" : {
                key1 : 'value1',
                key2 : 'value2'
            }
        });

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.be.equal(6);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.be.equal(6);
        expect(Object.keys(exp.ExpressionAttributeNames).sort().join()).to.be.equal(
            ['#string','#number','#object','#array','#double','#date'].sort().join()
        )
        expect(Object.keys(exp.ExpressionAttributeValues).sort().join()).to.be.equal(
            [':string_0',':number_0',':object_0',':array_0',':double_0',':date_0'].sort().join()
        )
        expect((exp.Expression.match(/AND/g) || []).length).to.be.equal(5);
        expect(exp.Expression).to.be.equal(
            '#string [op] :string_0 AND #number [op] :number_0 AND #date [op] :date_0 AND #double [op] :double_0 AND #array = :array_0 AND #object = :object_0'.replace(/\[op\]/g, '=')
        )
    }

    @test 'DynamoDB FilterExpression : should generate simple query with multiple keys'() {
        __ops1.forEach(
            op => {
                let exp = new Query();
                exp.build({ 
                    "string" : {
                        [op] : "string"
                    },
                    "number" : {
                        [op] : 1
                    },
                    "date" : {
                        [op] : (new Date()).toISOString()
                    },
                    "double" : {
                        [op] : 1.5
                    },
                    "array" : {
                        '$eq' : [1,2,3]
                    },
                    "object" : {
                        '$eq' : {
                            key1 : 'value2',
                            key2 : 'value2'
                         }
                    }
                });

                expect(Object.keys(exp.ExpressionAttributeNames).length).to.be.equal(6);
                expect(Object.keys(exp.ExpressionAttributeValues).length).to.be.equal(6);
                expect(Object.keys(exp.ExpressionAttributeNames).sort().join()).to.be.equal(
                    ['#string','#number','#object','#array','#double','#date'].sort().join()
                )
                expect(Object.keys(exp.ExpressionAttributeValues).sort().join()).to.be.equal(
                    [':string_0',':number_0',':object_0',':array_0',':double_0',':date_0'].sort().join()
                )
                expect((exp.Expression.match(/AND/g) || []).length).to.be.equal(5);
                expect(exp.Expression).to.be.equal(
                    '#string [op] :string_0 AND #number [op] :number_0 AND #date [op] :date_0 AND #double [op] :double_0 AND #array = :array_0 AND #object = :object_0'.replace(/\[op\]/g, exp.comperators[op])
                )
            }
        )
    }

    @test 'DynamoDB FilterExpression : should generate $or query of signle key'() {
        let exp = new Query();
        exp.build({ $or : [
            { balance : { $gt : 1000, $ne : 5000 } },
            { balance : { $lt : 2000, $ne : 0 } },
        ]});

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.be.equal(1);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.be.equal(4);
        expect(exp.ExpressionAttributeNames).to.haveOwnProperty('#balance');
        expect(exp.ExpressionAttributeNames['#balance']).to.be.equal('balance');
        expect(exp.ExpressionAttributeValues[':balance_0']).to.be.equal(1000);
        expect(exp.ExpressionAttributeValues[':balance_1']).to.be.equal(5000);
        expect(exp.ExpressionAttributeValues[':balance_2']).to.be.equal(2000);
        expect(exp.ExpressionAttributeValues[':balance_3']).to.be.equal(0);
        expect(exp.Expression).to.be.equal(
            '( #balance > :balance_0 AND #balance <> :balance_1 ) OR ( #balance < :balance_2 AND #balance <> :balance_3 )'
        );
    }

    @test 'DynamoDB FilterExpression : should generate nested complex $or query of $ands'() {
        let exp = new Query();
        exp.build({ $or : [
            { 
                $and : [
                    { balance : { $gt : 1000 } },
                    { balance : { $lt : 2000 } }
                ]
            },
            {
                $and : [
                    { quantity : { $ne : 0 } },
                    { quantity : { $ne : 5000 } },
                    { product : { $in : ['book', 'CD'] } },
                    { stat : { $nin : ['old', 'used'] } },
                    { author : { $not : { $ne : 'abc' } } },
                    { $or : [
                        { stars : 5 },
                        { stars : { $exists : 0 } }
                    ]}
                ]
            }
        ]});

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.equal(6);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.equal(10);
        expect(exp.Expression).to.equal(
            '#balance > :balance_0 AND #balance < :balance_1 OR #quantity <> :quantity_0 AND #quantity <> :quantity_1 AND #product IN (:product_0_0,:product_0_1) AND NOT ( #stat IN (:stat_0_0,:stat_0_1) ) AND #author = :author_0 AND ( #stars = :stars_0 OR attribute_not_exists(#stars) )'
        );
    }

    @test 'DynamoDB FilterExpression : $and query with empty subquery'() {
        let exp = new Query();
        exp.build({ '$and': [ {}, { _p_user: '_User$vtp2pCZmv2' } ],
            _rperm: { '$in': [ null, '*', 'vtp2pCZmv2' ] } });

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.equal(2);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.equal(4);
        expect(exp.ExpressionAttributeValues[':null']).to.equal(null);
        expect(exp.Expression).to.equal(
            '#p_user = :p_user_0 AND ( contains(#rperm,:rperm_0_0) OR contains(#rperm,:rperm_0_1) OR attribute_not_exists(#rperm) OR #rperm = :null )'
        );
    }

    @test 'DynamoDB FilterExpression : applying more than one query on same key 1'() {
        let exp = new Query();
        exp.build({ _id: { '$eq': 'unWCHGvGFE', '$nin': [ '8q4qVagG1h', 'unWCHGvGFE' ] },
                    _rperm: { '$in': [ null, '*', '3sRA9jCEGC' ] } });

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.equal(2);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.equal(6);
        expect(exp.ExpressionAttributeValues[':null']).to.equal(null);
        expect(exp.Expression).to.equal(
            '#id = :id_0 AND NOT ( #id IN (:id_1_0,:id_1_1) ) AND ( contains(#rperm,:rperm_0_0) OR contains(#rperm,:rperm_0_1) OR attribute_not_exists(#rperm) OR #rperm = :null )'
        );
    }

    @test 'DynamoDB FilterExpression : applying more than one query on same key 2'() {
        let exp = new Query();
        exp.build({ _id: 
            { '$in': 
                [   'G0kOG5lMpz',
                    'eLrvwTQ25l',
                    'NDteakhAun',
                    'NDteakhAun',
                    'eLrvwTQ25l' ],
                '$nin': [ 'NDteakhAun', 'eLrvwTQ25l' ] 
            } 
        });

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.equal(1);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.equal(7);
        expect(exp.Expression).to.equal(
            '#id IN (:id_0_0,:id_0_1,:id_0_2,:id_0_3,:id_0_4) AND NOT ( #id IN (:id_5_0,:id_5_1) )'
        );
    }

    @test 'DynamoDB FilterExpression : can do $all containsAll'() {
        let exp = new Query();
        exp.build({ numbers: { '$all': [ 1, 2, 3 ] } });

        expect(Object.keys(exp.ExpressionAttributeNames).length).to.equal(1);
        expect(Object.keys(exp.ExpressionAttributeValues).length).to.equal(3);
        expect(exp.Expression).to.equal(
            '( contains(#numbers,:numbers_0_0) AND contains(#numbers,:numbers_0_1) AND contains(#numbers,:numbers_0_2) )'
        );
    }

    @test 'DynamoDB UpdateExpression : set one attribute'() {
        let params = {};
        let exp = Query.getUpdateExpression({
            foo : 'bar'
        }, params);
        let vl = Object.keys(params['ExpressionAttributeValues'])[0];

        expect(exp).to.be.equal('SET #foo = ' + vl);

        params = {};
        exp = Query.getUpdateExpression({
            $set : {
                foo : 'bar'
            }
        }, params)
        vl = Object.keys(params['ExpressionAttributeValues'])[0];
        
        expect(exp).to.be.equal('SET #foo = ' + vl);
    }

    @test 'DynamoDB UpdateExpression : inc one attribute'() {
        let params = {};
        let exp = Query.getUpdateExpression({
            $inc : {
                foo : 1
            }
        }, params);
        let vl = Object.keys(params['ExpressionAttributeValues'])[0];

        expect(exp).to.be.equal('SET #foo = if_not_exists(#foo,:__zero__) + :foo_0');
    }
}