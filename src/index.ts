import { Partition, FilterExpression } from './DynamoPartition';
import { Adapter } from './DynamoAdapter';

export class DynamoDB {
    Partition;
    Query;

    Adapter : Adapter;

    constructor(readonly database, readonly settings) {
        this.database = database;
        this.settings = settings;
        this.Partition = Partition;
        this.Query = FilterExpression;
    }

    getAdapter() {
        if (this.Adapter) {
            return this.Adapter
        }

        this.Adapter = new Adapter(this.database, this.settings);
        return this.Adapter;
    }
}