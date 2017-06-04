import { Partition, FilterExpression } from './DynamoPartition';
import { Adapter } from './DynamoAdapter';

class DynamoDB {
    Partition = Partition
    Query = FilterExpression

    Adapter : Adapter;

    getAdapter(database, setting) {
        if (!this.Adapter) {
            this.Adapter = new Adapter(database, setting);
        }

        this.Adapter = new Adapter(database, setting);
        return this.Adapter;
    }
}

export {
    DynamoDB,
    Partition,
    FilterExpression
}