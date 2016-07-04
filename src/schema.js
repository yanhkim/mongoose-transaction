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
const ONE_MINUTE = 60 * 1000;

const TransactionSchema = new mongoose.Schema({
    history: [],
    state: { type: String, required: true, 'default': 'pending' },
});

TransactionSchema.TRANSACTION_COLLECTION = 'Transaction';
TransactionSchema.RAW_TRANSACTION_COLLECTION =
    mongooseutils.toCollectionName(TransactionSchema.TRANSACTION_COLLECTION);
TransactionSchema.TRANSACTION_EXPIRE_GAP = ONE_MINUTE;

TransactionSchema.attachShardKey = (doc) => {
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
    const TransactionModel = this;
    const promise = (async() => {
        let transaction = new TransactionModel();
        await transaction.begin();
        return transaction;
    })();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
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
    this.state = 'pending';
    this._docs = [];

    const promise = (async() => {
        DEBUG('transaction begin', this._id);
        TransactionSchema.attachShardKey(this);
        await helper.save(this);
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
TransactionSchema.methods.add = async function(doc, callback) {
    const promise = (async() => {
        if (Array.isArray(doc)) {
            let promises = doc.map(async(d) => this.add(d));
            await Promise.all(promises);
            return;
        }

        let pseudoModel = ModelMap.getPseudoModel(doc);
        TransactionSchema.attachShardKey(this);

        await helper.validate(doc);
        if (doc.isNew) {
            // create new document
            let data = {_id: doc._id, t: this._id, __new: true};
            utils.addShardKeyDatas(pseudoModel, doc, data);
            await helper.insert(doc.collection, data);
            this._docs.push(doc);
            return;
        }
        if (doc.t && !DEFINE.NULL_OBJECTID.equals(doc.t) &&
                doc.t.toString() !== this._id.toString()) {
            let hint = {collection: doc && ModelMap.getCollectionName(doc),
                        doc: doc && doc._id,
                        transaction: this};
            throw new TransactionError(ERROR_TYPE.BROKEN_DATA, hint);
        }
        if (doc.t && !DEFINE.NULL_OBJECTID.equals(doc.t)) {
            // FIXME: preload transacted
            return;
        }
        let query = {_id: doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                         {t: {$exists: false}}]};
        utils.addShardKeyDatas(pseudoModel, doc, query);
        let update = {$set: {t: this._id}};
        await atomic.acquireLock(pseudoModel.connection.db,
                                 doc.collection.name, query, update);
        this._docs.push(doc);
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
    const promise = (async() => {
        ModelMap.getPseudoModel(doc);
        doc.isRemove = true;
        await this.add(doc);
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
// #### Return
// :Promise:
//
// #### Transaction errors
// * 41 - state already changed; process conflict
TransactionSchema.methods._moveState = async function(prev, delta) {
    let transactionModel = ModelMap.getPseudoModel(
        TransactionSchema.RAW_TRANSACTION_COLLECTION
    );
    let query = {_id: this._id, state: prev};
    TransactionSchema.attachShardKey(query);

    let [numberUpdated, collection] = await atomic.pseudoFindAndModify(
            transactionModel.connection.db,
            TransactionSchema.RAW_TRANSACTION_COLLECTION,
            query, delta
    );
    if (numberUpdated === 1) {
        return;
    }
    // if `findAndModify` return wrong result,
    // it only can wrong query case.
    let modQuery = utils.unwrapMongoOp(utils.wrapMongoOp(query));
    delete modQuery.state;

    let updatedDoc = await helper.findOne(collection, modQuery);
    let state1 = delta.state || ((delta.$set || {}).state);
    let state2 = updatedDoc && updatedDoc.state;
    if (state1 !== state2) {
        throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG,
                                   {transaction: this, query: query});
    }
    return (updatedDoc && updatedDoc.history) || [];
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
    const promise = (async() => {
        await this._commit();
        let errors = [];

        let promises = this.history.map(async(history) => {
            let pseudoModel;
            try {
                pseudoModel = ModelMap.getPseudoModel(history.col);
            } catch (e) {
                errors.push(e);
                return;
            }
            let query = {_id: history.id, t: this._id};
            utils.addShardKeyDatas(pseudoModel, history, query);
            if (history.options && history.options.remove) {
                let col = pseudoModel.connection.collection(history.col);
                try {
                    await helper.remove(col, query);
                } catch (e) {
                    errors.push(e);
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
            } catch (e) {
                errors.push(e);
            }
        });
        await Promise.all(promises);

        if (errors.length) {
            console.error('errors', errors);
            // TODO: cleanup batch
            return;
        }
        await helper.remove(this.collection, {_id: this._id});
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
// #### Return
// :Promise:
//
// #### Promise arguments
// * errors
TransactionSchema.methods._makeHistory = async function() {
    let errors = [];

    if (this.history.length) {
        return errors;
    }

    let promises = this._docs.map(async(doc) => {
        let err;
        try {
            await helper.validate(doc);
        } catch (e) {
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
        this.history.push(history);
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

    await Promise.all(promises);

    return errors;
};

// ### Transaction._commit
// Transaction state move to `commit`
//
// This method save :Transaction._makeHistory:'s result
//
// #### Return
// * :Promise:
//
// #### Transaction errors
// * 44 - already expired at another process
// * 60 - unknown error
//
// SeeAlso :Transaction._moveState:
TransactionSchema.methods._commit = async function() {
    if (this.state === 'commit') {
        DEBUG('retry transaction commit', this._id);
        return;
    }

    if (this.state === 'expire') {
        DEBUG('transaction expired', this._id);
        throw new TransactionError(ERROR_TYPE.TRANSACTION_EXPIRED,
                                   {transaction: this})
    }

    let hint = {transaction: this};
    let errors = await this._makeHistory();
    if (errors.length) {
        console.error(errors);
        await this.expire();
        if (errors[0]) {
            throw errors[0];
        }
        throw new TransactionError(ERROR_TYPE.UNKNOWN_COMMIT_ERROR,
                                   hint);
    }
    if (this.isExpired()) {
        await this.expire();
        throw new TransactionError(ERROR_TYPE.TRANSACTION_EXPIRED, hint);
    }
    this.state = 'commit';
    DEBUG('transaction commit', this._id);
    let delta = utils.extractDelta(this);
    let history;
    try {
        history = await this._moveState('pending', delta);
    } catch (e) {
        // this case only can db error or already expired
        this.state = undefined;
        await this.expire();
        throw new TransactionError(ERROR_TYPE.UNKNOWN_COMMIT_ERROR, hint);
    }
    if (!history) {
        this._docs.forEach(function(doc) {
            doc.emit('transactionAdded', this);
        });
    } else {
        this.history = history;
    }
    this.$__reset();
};

// ### Transaction._expire
// Transaction state move to `expire`
//
// This method save :Transaction._makeHistory:'s result
//
// #### Return
// :Promise:
//
// #### Transaction errors
// SeeAlso :Transaction._moveState:
TransactionSchema.methods._expire = async function() {
    DEBUG('transaction expired', this._id);

    if (this.state === 'expire') {
        DEBUG('retry transaction expired', this._id);
        return;
    }
    if (this.state === 'commit') {
        DEBUG('transaction committed', this._id);
        return;
    }
    await this._makeHistory();
    this.state = 'expire';
    let delta = utils.extractDelta(this);
    let history = await this._moveState('pending', delta);
    if (history) {
        this.history = history;
    }
    this.$__reset();
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
    const promise = (async() => {
        await this._expire();
        let errors = [];

        let promises = this.history.map(async(history) => {
            if (!history.op) {
                await utils.sleep(-1);
                return;
            }
            let pseudoModel;
            try {
                pseudoModel = ModelMap.getPseudoModel(history.col);
            } catch (e) {
                errors.push(e);
                return;
            }

            let query = {_id: history.id, t: this._id};
            utils.addShardKeyDatas(pseudoModel, history, query);
            if (history.options && history.options.new) {
                let col = pseudoModel.connection.collection(history.col);
                try {
                    await helper.remove(col, query);
                } catch (e) {
                    errors.push(e);
                }
                return;
            }

            try {
                await atomic.releaseLock(pseudoModel.connection.db,
                                         history.col, query,
                                         {$set: {t: DEFINE.NULL_OBJECTID}});
            } catch (e) {
                errors.push(e);
            }
        });
        await Promise.all(promises);
        if (errors.length) {
            console.error('errors', errors);
            // TODO: cleanup batch
            return;
        }
        await helper.remove(this.collection, {_id: this._id});
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
    const promise = (async() => {
        await this.expire();
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
// #### Return
// :Promise:
//
// #### Transaction errors
// * 43 - conflict another transaction; transaction still alive
TransactionSchema.methods._postProcess = async function() {
    switch (this.state) {
        case 'pending':
            if (this.isExpired()) {
                await this.expire();
                return;
            }
            let hint = {collection: ModelMap.getCollectionName(this),
                        transaction: this};
            throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2,
                                       hint);
        case 'commit':
            await this.commit();
            return;
        case 'expire':
            await this.expire();
            return;
        default:
            throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG);
    }
};

TransactionSchema.methods.convertQueryForAvoidConflict = (conditions) => {
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
    let callback;
    if (typeof args[args.length - 1] === 'function') {
        callback = args[args.length - 1];
        args.pop();
    }

    let [conditions, options] = args;
    conditions = conditions || {};
    options = options || {};

    conditions.$or = conditions.$or || [];
    conditions.$or = conditions.$or.concat([{t: DEFINE.NULL_OBJECTID},
                                            {t: {$exists: false}}]);

    const promise = (async() => {
        let pseudoModel = ModelMap.getPseudoModel(model);

        let cursor = await helper.find(model.collection, conditions, options);
        let docs = await helper.toArray(cursor);
        if (!docs) {
            return;
        }
        let RETRY_LIMIT = 5;
        let locked = [];
        let promises = docs.map(async(_doc) => {
            let query = {_id: _doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                              {t: {$exists: false}}]};
            utils.addShardKeyDatas(pseudoModel, _doc, query);
            let query2 = {_id: _doc._id};
            utils.addShardKeyDatas(pseudoModel, _doc, query2);
            let lastError;
            for (let i = 0; i < RETRY_LIMIT; i += 1) {
                let doc;
                let opt = {new: true, fields: options.fields};
                try {
                    doc = await helper.findOneAndUpdate(model, query,
                                                        {$set: {t: this._id}},
                                                        opt);
                } catch (e) {
                    lastError = e;
                };
                if (doc) {
                    this._docs.push(doc);
                    locked.push(doc);
                    return;
                }
                await utils.sleep(_.sample([37, 59, 139]));
            }
            let hint = {
                collection: _doc && ModelMap.getCollectionName(_doc),
                doc: _doc && _doc._id,
                query: query,
                transaction: this,
            };
            lastError = lastError ||
                        new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2,
                                             hint);
            throw lastError;
        });
        await Promise.all(promises);
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
    let callback;
    if (typeof args[args.length - 1] === 'function') {
        callback = args[args.length - 1];
        args.pop();
    }

    let [conditions, options] = args;
    conditions = conditions || {};
    options = options || {};

    const promise = (async() => {
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
        for (let i = 0; i < RETRY_LIMIT; i += 1) {
            let doc;
            try {
                doc = await helper.findOneAndUpdate(model, query1,
                                                    {$set: {t: this._id}},
                                                    {new: true,
                                                     fields: options.fields});
            } catch (e) {}

            if (doc) {
                this._docs.push(doc);
                return doc;
            }

            // if t is not NULL_OBJECTID, try to go end of transaction process
            try {
                await helper.findOne(model, query2);
            } catch (e) {}

            await utils.sleep(_.sample([37, 59, 139]));
        }

        let hint = {collection: _doc && ModelMap.getCollectionName(_doc),
                    doc: _doc && _doc._id,
                    query: query1,
                    transaction: this};
        throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2, hint);
    })();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
};

module.exports = TransactionSchema;
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
