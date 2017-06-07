import { Partition, FilterExpression } from './DynamoPartition';
import { Adapter } from './DynamoAdapter';

class DynamoDB {
    Partition = Partition;
    QueryExpression = FilterExpression;

    Adapter : Adapter;

    constructor(readonly database, readonly settings) {
        this.database = database;
        this.settings = settings;
    }

    getAdapter() {
        if (this.Adapter) {
            return this.Adapter
        }

        this.Adapter = new Adapter(this.database, this.settings);
        return this.Adapter;
    }
}

export {
    DynamoDB
}