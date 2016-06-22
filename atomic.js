// ## PseudoFindAndModify
var sync = require('synchronize');
var utils = require('./utils');
var TransactionError = require('./error');
var DEFINE = require('./define');
var ERROR_TYPE = DEFINE.ERROR_TYPE;

// ### pseudoFindAndModify
// Emulate `findAndModify` of mongo native query using `update` and `find`
// combination.
//
// It can give more perfomance than original on the shard environ.
//
// #### Arguments
// * db - :Db: SeeAlso `node-mongodb-native/lib/db.js`
// * collectionName - :String:
// * query - :Object:
// * update - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
var pseudoFindAndModify = function(db, collectionName, query, update,
                                   callback) {
    var _writeOptions = {
        w: 1 //, write concern,
        // wtimeout: 0, // write concern wait timeout
        // fsync: false, // write waits for fsync
        // journal: false, // write waits for journal sync
    };

    sync.fiber(function() {
        db.collection(collectionName, sync.defer());
        var collection = sync.await();
        collection.update(query, update, _writeOptions, sync.defer());
        var numberUpdated = sync.await();
        return {collection: collection, numberUpdated: numberUpdated};
    }, function(err, ret) {
        if (err) {
            return callback(err);
        }
        callback(null, ret.numberUpdated, ret.collection);
    });
};

// ### acquireTransactionLock
// Only can use set `t` value to document or save without transaction.
//
// `query` must have **`t: NULL_OBJECTID`** condition
//
// #### Arguments
// * db - :Db: SeeAlso `node-mongodb-native/lib/db.js`
// * collectionName - :String:
// * query - :Object:
// * update - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// * 41 - cannot found update target document
// * 42 - conflict another transaction; document locked
var acquireTransactionLock = function(db, collectionName, query, update,
                                      callback) {
    callback = callback || function() {};
    sync.fiber(function() {
        pseudoFindAndModify(db, collectionName, query, update,
                            sync.defers('numberUpdated', 'collection'));
        var __ = sync.await();
        if (__.numberUpdated == 1) {
            return;
        }
        var modQuery = utils.unwrapMongoOp(utils.wrapMongoOp(query));
        // delete modQuery.t;
        if (modQuery.$or) {
            modQuery.$or = modQuery.$or.filter(function (cond) {
                return !cond.t;
            });
            if (modQuery.$or.length === 0) {
                delete modQuery.$or;
            }
        }
        // if findOne return wrong result,
        // `t` value changed to the another transaction
        __.collection.findOne(modQuery, {_id: 1, t: 1}, sync.defer());
        var updatedDoc = sync.await();
        var t1 = String(update.t || ((update.$set || {}).t));
        var t2 = String(updatedDoc && updatedDoc.t);
        if (t1 == t2) {
            return;
        }
        if (!updatedDoc) {
            throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG,
                                       {collection: collectionName,
                                        doc: query._id, query: query});
        }
        throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_1,
                                   {collection: collectionName,
                                    doc: query._id, query: query});
    }, callback);
};

// ### releaseTransactionLock
// Only can use unset `t` value to document
//
// `query` must have **`t: ObjectId(...)** condition,
// and `update` must have **`$set: {t: NULL_OBJECTID}`**
//
// #### Arguments
// * db - :Db: SeeAlso `node-mongodb-native/lib/db.js`
// * collectionName - :String:
// * query - :Object:
// * update - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
var releaseTransactionLock = function(db, collectionName, query, update,
                                      callback) {
    callback = callback || function() {};
    sync.fiber(function() {
        pseudoFindAndModify(db, collectionName, query, update,
                            sync.defers('numberUpdated', 'collection'));
        var __ = sync.await();
        if (__.numberUpdated == 1) {
            return;
        }
        // if findAndModify return wrong result,
        // it only can wrong query case.
        __.collection.findOne(query, {_id: 1, t: 1}, sync.defer());
        var doc = sync.await();
        if (!doc) {
            return;
        }
        // if function use on the transaction base, should'nt find document.
        // TODO: need cross check update field.
        throw new Error('Transaction.commit> no matching document for commit');
    }, callback);
};

// ### findAndModifyMongoNative
// update document
//
// #### Arguments
// * connection - :Connection:
// * collection - :MongoCollection:
// * query - :Object:
// * update - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
// * doc - :Object:
var findAndModifyMongoNative = function(connection, collection, query, update,
                                        fields, callback) {
    sync.fiber(function() {
        var data;
        // below 3.6.x
        if (DEFINE.MONGOOSE_VERSIONS[0] < 3 ||
                (DEFINE.MONGOOSE_VERSIONS[0] == 3 &&
                 DEFINE.MONGOOSE_VERSIONS[1] <= 6)) {
            connection.db.executeDbCommand({findAndModify: collection.name,
                                            query: query,
                                            update: update,
                                            fields: fields,
                                            new: true},
                                            sync.defer());
            data = sync.await();
            if (!data || !data.documents || !data.documents[0]) {
                throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG,
                                           {collection: collection.name,
                                            query: query, update: update});
            }
            return data.documents[0].value;
        }
        collection.findAndModify(query, [], update,
                                 {fields: fields, new: true},
                                 sync.defer());
        data = sync.await();
        // above to 3.7.x less than 4.x
        if (DEFINE.MONGOOSE_VERSIONS[0] < 4) {
            return data;
        }
        if (!data) {
            throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG,
                                       {collection: collection.name,
                                        query: query, update: update});
        }
        // above 4.x
        return data.value;
    }, callback);
};

module.exports = {
    pseudoFindAndModify: pseudoFindAndModify,
    acquireLock: acquireTransactionLock,
    releaseLock: releaseTransactionLock,
    findAndModify: findAndModifyMongoNative,
}
