import { Partition } from './Partition';
import { Expression } from './Expression';
import { Adapter } from './Adapter';

export class DynamoDB {

    Partition;
    Expression;

    Adapter : Adapter;

    constructor(readonly database, readonly settings) {
        this.database = database;
        this.settings = settings;
        this.Partition = Partition;
        this.Expression = Expression;
    }

    getAdapter() {
        if (this.Adapter) {
            return this.Adapter
        }

        this.Adapter = new Adapter(this.database, this.settings);
        return this.Adapter;
    }
}