// ## Transaction
// ### State
// All transaction start within `pending` state.
// It will delete after `commit` or `expire` states.
//
//         pending (create transaction document)
//            |
//         .----.
//         v    v
//      commit expire
//         |    |
//         `----'
//            |
//            v
//          done (delete transaction document)
//
// ### Legacy
// this project has some legacy codes. These codes need to care
// mongo/mongoose's buggy actions
//
// #### findAndModify
// Transaciton require `mongo native atomic` actions, so we only can use
// `findAndModify` or `update` command.
//
// but `findAndModify` command will lock of all shard collections,
// make `pseudo findAndModify` using combine two commands(`update`, `findOne`)
//
// #### update
// Some `specific` mongodb's `update` command return buggy result.
//
// - data write finished, `update` return updated documents count is 0.
// - mongoose use this count, it raise error
//
// So, we decide if update result count is 0, send `findOne` command,
// recheck document change.
'use strict';
const Promise = require('songbird');
const async = require('async');
const sync = require('synchronize');
const mongoose = require('mongoose');
const mongooseutils = require('mongoose/lib/utils');
const _ = require('underscore');
const atomic = require('./atomic');
const utils = require('./utils');
const helper = require('./helper');
const TransactionError = require('./error');
const DEFINE = require('./define');
const ModelMap = require('./modelmap');
const ERROR_TYPE = DEFINE.ERROR_TYPE;
const DEBUG = utils.DEBUG;
const toCollectionName = mongooseutils.toCollectionName;
const ONE_MINUTE = 60 * 1000;

const TransactionSchema = new mongoose.Schema({
    history: [],
    state: { type: String, required: true, 'default': 'pending' },
});

TransactionSchema.TRANSACTION_COLLECTION = 'Transaction';
TransactionSchema.RAW_TRANSACTION_COLLECTION =
    toCollectionName(TransactionSchema.TRANSACTION_COLLECTION);
TransactionSchema.TRANSACTION_EXPIRE_GAP = ONE_MINUTE;

TransactionSchema.attachShardKey = function(doc) {
    if (!TransactionSchema.options.shardKey ||
            !TransactionSchema.options.shardKey.initialize) {
        return;
    }
    TransactionSchema.options.shardKey.initialize(doc);
};

// ### Transaction.new
// Create new transaction
//
// :staticmethod:
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
// * doc - :Document:
TransactionSchema.statics.new =
TransactionSchema.statics.begin = async function(callback) {
    let transaction = new this();
    transaction.begin(function(err) {
        callback(err, transaction);
    });
};

// ### Transaction.isExpired
// Check transaction expiring
//
// If old transaction, process decide `deadlock`
//
// #### Return
// :Boolean:
TransactionSchema.methods.isExpired = function isExpired() {
    let issued = this._id.getTimestamp();
    let now = new Date();
    let diff = now - issued;
    return TransactionSchema.TRANSACTION_EXPIRE_GAP < diff;
};

// ### Transaction.begin
// Start transaction mode
//
// Save transaction document with `pending` state to mongodb
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods.begin = async function(callback) {
    let self = this;
    self.state = 'pending';
    self._docs = [];

    let promise = (async() => {
        DEBUG('transaction begin', self._id);
        TransactionSchema.attachShardKey(self);
        await helper.save(self);
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction.add
// Target document marking for transact manage
//
// Modify `t` value of document and save to mongodb
//
// #### Arguments
// * doc - :Document:
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// * 40 - document already has `t` value, value is not match transaction's id
// * 50 - unknown colllection
//
// SeeAlso :atomic.acquireTransactionLock:
TransactionSchema.methods.add = async function (doc, callback) {
    let self = this;

    let promise = (async() => {
        if (Array.isArray(doc)) {
            await Promise.each(doc, async(d) => {
                await self.add(d);
            });
            return;
        }

        let pseudoModel = ModelMap.getPseudoModel(doc);
        TransactionSchema.attachShardKey(self);

        await helper.validate(doc);
        if (doc.isNew) {
            // create new document
            let data = {_id: doc._id, t: self._id, __new: true};
            utils.addShardKeyDatas(pseudoModel, doc, data);
            await helper.insert(doc.collection, data);
            self._docs.push(doc);
            return;
        }
        if (doc.t && !DEFINE.NULL_OBJECTID.equals(doc.t) &&
                doc.t.toString() != self._id.toString()) {
            let hint = {collection: doc && ModelMap.getCollectionName(doc),
                        doc: doc && doc._id,
                        transaction: self};
            throw new TransactionError(ERROR_TYPE.BROKEN_DATA, hint);
        }
        if (doc.t && !DEFINE.NULL_OBJECTID.equals(doc.t)) {
            // FIXME: preload transacted
            return;
        }
        let query = {_id: doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                         {t: {$exists: false}}]};
        utils.addShardKeyDatas(pseudoModel, doc, query);
        let update = {$set: {t: self._id}};
        await atomic.acquireLock(pseudoModel.connection.db,
                                 doc.collection.name, query, update);
        self._docs.push(doc);
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction.removeDoc
// Target document making to remove
//
// #### Arguments
// * doc - :Document:
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// SeeAlso :Transaction.add:
TransactionSchema.methods.removeDoc = async function(doc, callback) {
    let self = this;

    let promise = (async() => {
        let pseudoModel = ModelMap.getPseudoModel(doc);
        doc.isRemove = true;
        await self.add(doc);
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction._moveState
// Change transaction state
//
// **`prev` is current state in db
// **`delta` must have `state: 'next_state'`
//
// #### Arguments
// * prev - :String:
// * delta - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// * 41 - state already changed; process conflict
TransactionSchema.methods._moveState = async function(prev, delta, callback) {
    let self = this;

    let promise = (async() => {
        let transactionModel = ModelMap.getPseudoModel(
            TransactionSchema.RAW_TRANSACTION_COLLECTION
        );
        let query = {_id: self._id, state: prev};
        TransactionSchema.attachShardKey(query);

        let [numberUpdated, collection] = await atomic.pseudoFindAndModify(
                transactionModel.connection.db,
                TransactionSchema.RAW_TRANSACTION_COLLECTION,
                query, delta
        );
        if (numberUpdated == 1) {
            return;
        }
        // if `findAndModify` return wrong result,
        // it only can wrong query case.
        let modQuery = utils.unwrapMongoOp(utils.wrapMongoOp(query));
        delete modQuery.state;

        let updatedDoc = await helper.findOne(collection, modQuery);
        let state1 = delta.state || ((delta.$set || {}).state);
        let state2 = updatedDoc && updatedDoc.state;
        if (state1 != state2) {
            throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG,
                                       {transaction: self, query: query});
        }
        return (updatedDoc && updatedDoc.history) || [];
    })();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
};

// ### Transaction.commit
// Commit all changes
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods.commit = async function(callback) {
    let self = this;

    let promise = (async() => {
        await self._commit();
        let errors = [];

        await Promise.each(self.history, async(history) => {
            let pseudoModel;
            try {
                pseudoModel = ModelMap.getPseudoModel(history.col);
            } catch(e) {
                errors.push(e);
                return;
            }
            let query = {_id: history.id, t: self._id};
            utils.addShardKeyDatas(pseudoModel, history, query);
            if (history.options && history.options.remove) {
                let col = pseudoModel.connection.collection(history.col);
                try {
                    await helper.remove(col, query);
                } catch(e) {
                    errors.push(err);
                }
                return;
            }
            if (!history.op) {
                await utils.sleep(-1);
                return;
            }
            let op = utils.unwrapMongoOp(JSON.parse(history.op));
            utils.removeShardKeySetData(pseudoModel.shardKey, op);
            try {
                await atomic.releaseLock(pseudoModel.connection.db,
                                         history.col, query, op);
            } catch(e) {
                errors.push(e);
            }
        });

        if (errors.length) {
            console.error('errors', errors);
            // TODO: cleanup batch
            return;
        }
        await helper.remove(self.collection, {_id: self._id});
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction._makeHistory
// Make `mongodb delta` of all changes
//
// This private method process below actions:
// * all document deltas mark processed
// * collected all deltas store transaction's history fields.
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods._makeHistory = async function(callback) {
    let self = this;
    let promise = (async() => {
        let errors = [];

        if (self.history.length) {
            return errors;
        }

        await Promise.each(self._docs || [], async(doc) => {
            let err;
            try {
                await helper.validate(doc);
            } catch(e) {
                err = e;
            }
            let history = {
                col: doc.collection.name,
                id: doc._id,
                options: {
                    new: doc.isNew,
                    remove: doc.isRemove,
                },
            };
            utils.addShardKeyDatas(ModelMap.getPseudoModel(doc), doc, history);
            self.history.push(history);
            if (err) {
                errors.push(err);
                // no need more action
                return;
            }

            let delta = utils.extractDelta(doc);
            delta.$set = delta.$set || {};
            delta.$set.t = DEFINE.NULL_OBJECTID;

            if (doc.isNew) {
                delta.$set = doc.toObject({depopulate: 1});
                if (!doc.$__.version) {
                    doc.$__version(true, delta.$set);
                }
                delete delta.$set._id;
                delta.$unset = {__new: 1};
            }

            DEBUG('DELTA', doc.collection.name, ':', JSON.stringify(delta));
            history.op = delta && JSON.stringify(utils.wrapMongoOp(delta));

            doc.$__reset();
        });

        return errors;
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction._commit
// Transaction state move to `commit`
//
// This method save :Transaction._makeHistory:'s result
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// * 44 - already expired at another process
// * 60 - unknown error
//
// SeeAlso :Transaction._moveState:
TransactionSchema.methods._commit = async function(callback) {
    let self = this;

    let promise = (async() => {
        if (self.state == 'commit') {
            DEBUG('retry transaction commit', self._id);
            return;
        }

        if (self.state == 'expire') {
            DEBUG('transaction expired', self._id);
            throw new TransactionError(ERROR_TYPE.TRANSACTION_EXPIRED,
                                       {transaction: self})
        }

        let hint = {transaction: self};
        let errors = await self._makeHistory();
        if (errors.length) {
            console.error(errors);
            await helper.promisify(self, self.expire)();
            if (errors[0]) {
                throw errors[0];
            }
            throw new TransactionError(ERROR_TYPE.UNKNOWN_COMMIT_ERROR,
                                       hint);
        }
        if (self.isExpired()) {
            await helper.promisify(self, self.expire)();
            throw new TransactionError(ERROR_TYPE.TRANSACTION_EXPIRED, hint);
        }
        self.state = 'commit';
        DEBUG('transaction commit', self._id);
        let delta = utils.extractDelta(self);
        let history;
        try {
            history = await self._moveState('pending', delta);
        } catch(e) {
            // this case only can db error or already expired
            self.state = undefined;
            await helper.promisify(self, self.expire)();
            throw new TransactionError(ERROR_TYPE.UNKNOWN_COMMIT_ERROR, hint);
        }
        if (!history) {
            self._docs.forEach(function (doc) {
                doc.emit('transactionAdded', self);
            });
        } else {
            self.history = history;
        }
        self.$__reset();
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction._expire
// Transaction state move to `expire`
//
// This method save :Transaction._makeHistory:'s result
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// SeeAlso :Transaction._moveState:
TransactionSchema.methods._expire = async function(callback) {
    let self = this;

    DEBUG('transaction expired', self._id);

    let promise = (async() => {
        if (self.state == 'expire') {
            DEBUG('retry transaction expired', self._id);
            return;
        }
        if (self.state == 'commit') {
            DEBUG('transaction committed', self._id);
            return;
        }
        await self._makeHistory();
        self.state = 'expire';
        let delta = utils.extractDelta(self);
        let history = await self._moveState('pending', delta);
        if (history) {
            self.history = history;
        }
        self.$__reset();
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction.expire
// Expire transaction
//
// This method mean same `rollback` of common db
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods.expire = async function(callback) {
    let self = this;

    let promise = (async() => {
        await self._expire();
        let errors = [];

        await Promise.each(self.history, async(history) => {
            if (!history.op) {
                await utils.sleep(-1);
                return;
            }
            let pseudoModel;
            try {
                pseudoModel = ModelMap.getPseudoModel(history.col);
            } catch(e) {
                errors.push(e);
                return;
            }

            let query = {_id: history.id, t: self._id};
            utils.addShardKeyDatas(pseudoModel, history, query);
            if (history.options && history.options.new) {
                let col = pseudoModel.connection.collection(history.col);
                try {
                    await helper.remove(col, query);
                } catch(e) {
                    errors.push(err);
                }
                return;
            }

            try {
                await atomic.releaseLock(pseudoModel.connection.db,
                                         history.col, query,
                                         {$set: {t: DEFINE.NULL_OBJECTID}});
            } catch(e) {
                errors.push(err);
            }
        });
        if (errors.length) {
            console.error('errors', errors);
            // TODO: cleanup batch
            return;
        }
        await helper.remove(self.collection, {_id: self._id});
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction.cancel
// Cancel transaction
//
// inputed `reason` return to callback`s `err`
//
// #### Arguments
// * reason - :String: or :Error:
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods.cancel = async function(reason, callback) {
    let self = this;
    let promise = (async() => {
        await self.expire();
        throw reason;
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

// ### Transaction._postProcess
// If transaction process had problem, continue process
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// * 43 - conflict another transaction; transaction still alive
TransactionSchema.methods._postProcess = async function(callback) {
    let self = this;
    let promise = (async() => {
        switch (self.state) {
            case 'pending':
                if (self.isExpired()) {
                    await self.expire();
                    return;
                }
                let hint = {collection: ModelMap.getCollectionName(self),
                            transaction: self};
                throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2,
                                           hint);
            case 'commit':
                await self.commit();
                return;
            case 'expire':
                await self.expire();
                return;
            default:
                throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG);
        }
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

TransactionSchema.methods.convertQueryForAvoidConflict =
        function getQueryForAvoidConflict(conditions) {
    conditions = conditions || {};
    conditions.$or = conditions.$or || [];
    conditions.$or = conditions.$or.concat([{t: DEFINE.NULL_OBJECTID},
                                            {t: {$exists: false}}]);
    return conditions;
};

// ### Transaction.find
// Find documents with assgined `t`
//
// If conflict another process, this method wait and retry action
//
// #### Arguments
// * model - :TransactedModel:
// * conditions - :Object:
// * options - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
// * docs - :Array:
//
// #### Transaction errors
// * 43 - conflict another transaction; document locked
// * 50 - unknown collection
TransactionSchema.methods.find = async function(model, ...args) {
    let self = this;
    let callback;
    if (typeof args[args.length - 1] == 'function') {
        callback = args[args.length - 1];
    }
    args.pop();

    let [conditions, options] = args;
    conditions = conditions || {};
    options = options || {};

    conditions.$or = conditions.$or || [];
    conditions.$or = conditions.$or.concat([{t: DEFINE.NULL_OBJECTID},
                                            {t: {$exists: false}}]);


    let promise = (async() => {
        let pseudoModel = ModelMap.getPseudoModel(model);

        let cursor = await helper.find(model.collection, conditions, options);
        let docs = await helper.toArray(cursor);
        if (!docs) {
            return;
        }
        let RETRY_LIMIT = 5;
        let locked = await Promise.map(docs, async(_doc) => {
            let query = {_id: _doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                              {t: {$exists: false}}]};
            utils.addShardKeyDatas(pseudoModel, _doc, query);
            let query2 = {_id: _doc._id};
            utils.addShardKeyDatas(pseudoModel, _doc, query2);
            let lastError;
            for (let i = 0; i < RETRY_LIMIT; i += 1) {
                let doc;
                try {
                    doc = await helper
                        .findOneAndUpdate(model, query, {$set: {t: self._id}},
                                          {new: true, fields: options.fields});
                } catch(e) {
                    lastError = e;
                };
                if (doc) {
                    self._docs.push(doc);
                    return doc;
                }
                await utils.sleep(_.sample([37, 59, 139]));
            }
            let hint = {
                collection: _doc &&
                            ModelMap.getCollectionName(_doc),
                doc: _doc && _doc._id,
                query: query,
                transaction: self
            };
            lastError = lastError ||
                        new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2,
                                             hint);
            throw lastError;
        });
        return locked;
    })();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
};

// ### Transaction.findOne
// Find documents with assgined `t`
//
// If conflict another process, this method wait and retry action
//
// #### Arguments
// * model - :TransactedModel:
// * conditions - :Object:
// * options - :Object:
// * callback - :Function:
//
// #### Callback arguments
// * err
// * doc - :Document:
//
// #### Transaction errors
// * 43 - conflict another transaction; transaction still alive
// * 50 - unknown collection
TransactionSchema.methods.findOne = async function(model, ...args) {
    let self = this;
    let callback;
    if (typeof args[args.length - 1] == 'function') {
        callback = args[args.length - 1];
    }
    args.pop();

    let [conditions, options] = args;
    conditions = conditions || {};
    options = options || {};

    let promise = (async() => {
        let pseudoModel = ModelMap.getPseudoModel(model);
        let _doc = await helper.findOne(model.collection, conditions, options);
        if (!_doc) {
            return;
        }

        let query1 = {_id: _doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                           {t: {$exists: false}}]};
        utils.addShardKeyDatas(pseudoModel, _doc, query1);
        let query2 = {_id: _doc._id};
        utils.addShardKeyDatas(pseudoModel, _doc, query2);
        let RETRY_LIMIT = 5;
        let lastError;
        for (let i = 0; i < RETRY_LIMIT; i += 1) {
            let doc;
            try {
                doc = await helper.findOneAndUpdate(model, query1,
                                                    {$set: {t: self._id}},
                                                    {new: true,
                                                     fields: options.fields});
            } catch(e) {}
            if (doc) {
                self._docs.push(doc);
                return doc;
            }

            // if t is not NULL_OBJECTID, try to go end of transaction process
            try {
                await helper.findOne(model, query2);
            } catch(e) {}

            await utils.sleep(_.sample([37, 59, 139]));
        }

        let hint = {collection: _doc &&
                                ModelMap.getCollectionName(_doc),
                    doc: _doc && _doc._id,
                    query: query1,
                    transaction: self};
        throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2, hint);
    })();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
};

module.exports = TransactionSchema;
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
