# mongoose-transaction
This project provide transaction feature between multiple connection/collection mongodb.

## Install

``` bash
npm install mongoose-transaction
```

## How to use

``` javascript
var mongoose = require('mongoose');
var transaction = require('mongoose-transaction');

var conn = mongoose.connect('mongodb://localhost:27017', function(err) {
    var TestSchema = new mongoose.Schema({
        data: Number,
    });
    var Transaction = connection.model(transaction.TRANSACTION_COLLECTION, transaction.TransactionSchema);
    var TestModel = transaction.TransactedModel(conn, 'Test', TestSchema);

    Transaction.begin(function(err, t) {
        var doc = new TestModel({data: 10});
        t.add(doc);
        t.commit(function(err) {
        });
    });
});
```
