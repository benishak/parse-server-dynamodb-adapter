# AWS DynamoDB Parse Server Adapter

this is database adapter to add support of AWS DynamoDB to Parse Server

## Setup

Create a Table using the AWS Console or the AWS API or the AWS CLI with the following parameters

- **Primary Key** : _pk_className
- **Sort Key** : _sk_id

YOU MUST USE THESE KEYS NAME IN ORDER TO USE THIS ADAPTER!!

### Example creating the table using the CLI
```
aws dynamodb create-table --table-name parse-server --attribute-definitions AttributeName=_pk_className,AttributeType=S AttributeName=_sk_id,AttributeType=S --key-schema AttributeName=_pk_className,KeyType=HASH AttributeName=_sk_id,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=500,WriteCapacityUnits=1000
```

Please read more on Read/Write Capacity Units [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ProvisionedThroughput.html)

Make sure you provision enough Capacity Units, it depends on your application, if your application is write intensive provision as twice as the read capacity units for your write capacity

The Read/Write Capacity Units can be changed anytime, but you cannot change the Primary Key and Sort Key once the table is created!

## Usage

```
var DynamoDB = require('parse-server-dynamodb-adapter').DynamoDB;
var dynamo = new DynamoDB('parse-server', // your DynamoDB Table 
              { apiVersion: '2012-08-10',
                region : 'eu-central-1',  // your AWS Region where you setup your DynamoDB Table
                accessKeyId: 'AK....',    // your AWS Access Key ID 
                secretAccessKey: 'secret' // your AWS Secret Access Key
               }
           );

var api = new ParseServer({
  databaseAdapter: dynamo.getAdapter(),
  appId: 'myAppId',
  masterKey: 'myMasterKey'
  serverURL: 'http://localhost:1337/parse'
  ...
});
```

## Limitation in AWS DynamoDB

Just like other databases DynamoDB has also some limitation which is documented [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html):
But the most important limitations that you need to know are 

- Maximum Document/Object/Item Size : 400KB (vs 16MB in MongoDB)
- Maximum number of elements in the `$in` query : 100 (vs unlimted in MongoDB)
- Maximum [Expression](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html) Length : 4 KB

# Compatibility with Parse Server

Please remember AWS DynamoDB is mainly a key-value Database, this adapter tried its best to simulate how Parse Server works with other Databases like MongoDB, however these features or functions won't work as you expect

- **Skip** : You cannot use `skip` on your Parse.Query
- **Sort** : If you use `ascending/descending` on any key other than `objectId` with your Parse.Query, the sort will be applied on the query **results**
- **containedIn** : this cannot support huge array more than `98` elements when using Parse.Query
- **containsAll** : you can use this with bigger array however remember that you may hit the maximum length of Expression, which is limited by 4KB
- **Uniqueness** : This adapter impelements uniqueness on the adapter layer, so uniquness is not 100% guaranteed when inserting data in parallel
- **read consistency** : DynamoDB has an eventually read consistency by default, but this query for the nature of Parse Server is using **strong read consistency**, this may come with extra AWS charges, read more about this topic [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html)
- **Array of Pointers** : you can use pointers but you cannot have an ***array of pointers***! we suggest storing only the `objectId` as string like this `Activity.set("users",["Xqcr55mjs", "Yc55wemx9", ... ]")` instead of `Activity.set("users", [User1, User2, User3, ... ])` 
