// ## PseudoFindAndModify
'use strict';
require('songbird');
const utils = require('./utils');
const TransactionError = require('./error');
const DEFINE = require('./define');
const ERROR_TYPE = DEFINE.ERROR_TYPE;

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

    const promise = (async() => {
        const collection = await db.collection(collectionName);
        let numberUpdated = await collection.promise.update(
            query,
            updateData,
            writeOptions,
        );
        if (numberUpdated.result) {
            numberUpdated = numberUpdated.result.nModified || 0;
        }
        return [numberUpdated, collection];
    })();

    if (callback) {
        return promise
            .then(ret => callback(null, ret[0], ret[1]))
            .catch(callback);
    }
    return promise;
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
    const promise = (async() => {
        const [numberUpdated, collection] = await pseudoFindAndModify(
            db,
            collectionName,
            query,
            updateData,
        );
        if (numberUpdated === 1) {
            return;
        }
        const modQuery = utils.unwrapMongoOp(utils.wrapMongoOp(query));
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
        const updatedDoc = await collection.promise.findOne(
            modQuery,
            {_id: 1, t: 1},
        );
        const t1 = String(updateData.t || ((updateData.$set || {}).t));
        const t2 = String(updatedDoc && updatedDoc.t);
        if (t1 === t2) {
            return;
        }
        const hint = {collection: collectionName, doc: query._id, query: query};
        throw new TransactionError(
            (
                updatedDoc
                    ? ERROR_TYPE.TRANSACTION_CONFLICT_1
                    : ERROR_TYPE.SOMETHING_WRONG
            ),
            hint,
        );
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
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
    const promise = (async() => {
        const [numberUpdated, collection] = await pseudoFindAndModify(
            db,
            collectionName,
            query,
            updateData,
        );
        if (numberUpdated === 1) {
            return;
        }
        // if findAndModify return wrong result,
        // it only can wrong query case.
        const doc = await collection.promise.findOne(query, {_id: 1, t: 1});
        if (!doc) {
            return;
        }
        // if function use on the transaction base, should'nt find document.
        // TODO: need cross check update field.
        throw new Error('Transaction.commit> no matching document for commit');
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

const findAndModifyMongoNativeOlder = async(connection, collection, query,
                                            updateData, fields) => {
    const command = {
        findAndModify: collection.name, query: query,
        update: updateData, fields: fields, new: true,
    };
    const data = await connection.db.promise.executeDbCommand(command);
    if (!data || !data.documents || !data.documents[0]) {
        throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG,
                                   {collection: collection.name,
                                    query: query, update: updateData});
    }
    return data.documents[0].value;
};

const findAndModifyMongoNativeNewer = async(collection, query, updateData,
                                            fields) => {
    const options = {fields: fields, new: true};
    const data = await collection.promise.findAndModify(query, [], updateData,
                                                        options);
    // above to 3.7.x less than 4.x
    if (DEFINE.MONGOOSE_VERSIONS[0] < 4) {
        return data;
    }
    // above 4.x
    if (!data) {
        const hint = {
            collection: collection.name,
            query: query,
            update: updateData,
        };
        throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG, hint);
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
    const promise = (async() => {
        // below 3.6.x
        if (DEFINE.MONGOOSE_VERSIONS[0] < 3 ||
                (DEFINE.MONGOOSE_VERSIONS[0] === 3 &&
                 DEFINE.MONGOOSE_VERSIONS[1] <= 6)) {
            return await findAndModifyMongoNativeOlder(connection, collection,
                                                       query, updateData,
                                                       fields);
        } else {
            return await findAndModifyMongoNativeNewer(collection, query,
                                                       updateData, fields);
        }
    })();

    if (callback) {
        return promise.then(ret => callback(null, ret)).catch(callback);
    }
    return promise;
};

module.exports = {
    pseudoFindAndModify: pseudoFindAndModify,
    acquireLock: acquireTransactionLock,
    releaseLock: releaseTransactionLock,
    findAndModify: findAndModifyMongoNative,
};
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
