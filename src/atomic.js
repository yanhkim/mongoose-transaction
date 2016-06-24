// ## PseudoFindAndModify
'use strict';
require('songbird');
const utils = require('./utils');
const TransactionError = require('./error');
const DEFINE = require('./define');
const ERROR_TYPE = DEFINE.ERROR_TYPE;

// old version mongo-native functions will stuck with async/await process
const update = (collection, query, updateData, writeOptions, callback) => {
    collection.update(query, updateData, writeOptions, callback);
};

const findOne = (collection, query, field, callback) => {
    collection.findOne(query, field, callback);
}

const findAndModify = (collection, query, sort, updateData, options,
                       callback) => {
    collection.findAndModify(query, sort, updateData, options, callback);
}

const executeDbCommand = (db, command, callback) => {
    db.executeDbCommand(command, callback);
}

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
// * updateData - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
// * numberUpdated - :Number:
// * collection - :Collection: SeeAlso `node-mongodb-native/lib/collection.js`
const pseudoFindAndModify = async(db, collectionName, query, updateData,
                                  callback) => {
    const writeOptions = {
        w: 1, // write concern,
        // wtimeout: 0, // write concern wait timeout
        // fsync: false, // write waits for fsync
        // journal: false, // write waits for journal sync
    };

    let collection;
    let numberUpdated;
    try {
        collection = await db.collection(collectionName);
        numberUpdated = await update.promise(collection, query, updateData,
                                             writeOptions);
    } catch (e) {
        if (callback) {
            return callback(e);
        }
        throw e;
    }
    callback && callback(null, numberUpdated, collection);
    return [numberUpdated, collection]
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
// * updateData - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// * 41 - cannot found update target document
// * 42 - conflict another transaction; document locked
const acquireTransactionLock = async(db, collectionName, query, updateData,
                                     callback) => {
    let numberUpdated, collection;
    try {
        [numberUpdated, collection] =
            await pseudoFindAndModify(db, collectionName, query, updateData);
    } catch (e) {
        if (callback) {
            return callback(e);
        }
        throw e;
    }
    if (numberUpdated === 1) {
        callback && callback();
        return;
    }
    let modQuery = utils.unwrapMongoOp(utils.wrapMongoOp(query));
    // delete modQuery.t;
    if (modQuery.$or) {
        modQuery.$or = modQuery.$or.filter((cond) => {
            return !cond.t;
        });
        if (modQuery.$or.length === 0) {
            delete modQuery.$or;
        }
    }
    // if findOne return wrong result,
    // `t` value changed to the another transaction
    let updatedDoc = await findOne.promise(collection, modQuery,
                                           {_id: 1, t: 1});
    let t1 = String(update.t || ((update.$set || {}).t));
    let t2 = String(updatedDoc && updatedDoc.t);
    if (t1 === t2) {
        callback && callback();
        return;
    }
    let hint = {collection: collectionName, doc: query._id, query: query};
    let err = new TransactionError((updatedDoc
                                        ? ERROR_TYPE.TRANSACTION_CONFLICT_1
                                        : ERROR_TYPE.SOMETHING_WRONG),
                                   hint);
    if (callback) {
        return callback(err);
    }
    throw err;
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
// * updateData - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
const releaseTransactionLock = async(db, collectionName, query, updateData,
                                     callback) => {
    let numberUpdated, collection;
    try {
        [numberUpdated, collection] =
            await pseudoFindAndModify(db, collectionName, query, updateData);
    } catch (e) {
        if (callback) {
            return callback(e);
        }
        throw e;
    }
    if (numberUpdated === 1) {
        callback && callback();
        return;
    }
    // if findAndModify return wrong result,
    // it only can wrong query case.
    let doc = await findOne.promise(collection, query, {_id: 1, t: 1});
    if (!doc) {
        callback && callback();
        return;
    }
    // if function use on the transaction base, should'nt find document.
    // TODO: need cross check update field.
    let err = new Error('Transaction.commit> no matching document for commit');
    if (callback) {
        return callback(err);
    }
    throw err;
};

const findAndModifyMongoNativeOlder = async(connection, collection, query,
                                            updateData, fields) => {
    let data = await executeDbCommand(connection.db,
                                      {findAndModify: collection.name,
                                       query: query,
                                       update: updateData,
                                       fields: fields,
                                       new: true});
    if (!data || !data.documents || !data.documents[0]) {
        throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG,
                                   {collection: collection.name,
                                    query: query, update: update});
    }
    return data.documents[0].value;
};

const findAndModifyMongoNativeNewer = async(collection, query, updateData,
                                            fields) => {
    let data = await findAndModify.promise(collection, query, [], updateData,
                                           {fields: fields, new: true});
    // above to 3.7.x less than 4.x
    if (DEFINE.MONGOOSE_VERSIONS[0] < 4) {
        return data;
    }
    // above 4.x
    if (!data) {
        throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG,
                                   {collection: collection.name,
                                    query: query, update: update});
    }
    return data.value;
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
const findAndModifyMongoNative = async(connection, collection, query,
                                       updateData, fields, callback) => {
    let data;
    try {
        // below 3.6.x
        if (DEFINE.MONGOOSE_VERSIONS[0] < 3 ||
                (DEFINE.MONGOOSE_VERSIONS[0] === 3 &&
                 DEFINE.MONGOOSE_VERSIONS[1] <= 6)) {
            data = await findAndModifyMongoNativeOlder(connection, collection,
                                                       query, updateData,
                                                       fields);
        } else {
            data = await findAndModifyMongoNativeNewer(collection, query,
                                                       updateData, fields);
        }
    } catch (e) {
        if (callback) {
            return callback(e);
        }
        throw e;
    }
    callback && callback(null, data);
    return data;
};

module.exports = {
    pseudoFindAndModify: pseudoFindAndModify,
    acquireLock: acquireTransactionLock,
    releaseLock: releaseTransactionLock,
    findAndModify: findAndModifyMongoNative,
};
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
