"use strict";
var async = require('async');
var sync = require('synchronize');
var mongoose = require('mongoose');
var mongooseutils = require('mongoose/lib/utils');
var _ = require('underscore');
var atomic = require('./atomic');
var utils = require('./utils');
var TransactionError = require('./error');
var DEFINE = require('./define');
var ModelMap = require('./modelmap');
var ERROR_TYPE = DEFINE.ERROR_TYPE;
var DEBUG = utils.DEBUG;
var toCollectionName = mongooseutils.toCollectionName;
var ONE_MINUTE = 60 * 1000;

var TransactionSchema = new mongoose.Schema({
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

// ## Transaction
//
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
TransactionSchema.statics.new = TransactionSchema.statics.begin =
        function(callback) {
    callback = callback || function() {};
    var transaction = new this();
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
    var issued = this._id.getTimestamp();
    var now = new Date();
    var diff = now - issued;
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
TransactionSchema.methods.begin = function(callback) {
    this.state = 'pending';
    this._docs = [];
    DEBUG('transaction begin', this._id);
    TransactionSchema.attachShardKey(this);
    this.save(callback);
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
TransactionSchema.methods.add = function (doc, callback) {
    callback = callback || function() {};
    var self = this;
    if (Array.isArray(doc)) {
        return async.each(doc, function(d, next) {
            self.add(d, next);
        }, callback);
    }
    var pseudoModel = ModelMap.getPseudoModel(doc);
    TransactionSchema.attachShardKey(self);
    sync.fiber(function() {
        doc.validate(sync.defer()); sync.await();
        if (doc.isNew) {
            // create new document
            var data = {_id: doc._id, t: self._id, __new: true};
            utils.addShardKeyDatas(pseudoModel, doc, data);
            doc.collection.insert(data, sync.defer()); sync.await();
            self._docs.push(doc);
            return;
        }
        if (doc.t && !DEFINE.NULL_OBJECTID.equals(doc.t) &&
                doc.t.toString() != self._id.toString()) {

            var hint = {collection: doc && ModelMap.getCollectionName(doc),
                        doc: doc && doc._id,
                        transaction: self};
            throw new TransactionError(ERROR_TYPE.BROKEN_DATA,
                                       hint);
        }
        if (doc.t && !DEFINE.NULL_OBJECTID.equals(doc.t)) {
            // FIXME: preload transacted
            return;
        }
        var query = {_id: doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
            {t: {$exists: false}}]};
        utils.addShardKeyDatas(pseudoModel, doc, query);
        var update = {$set: {t: self._id}};
        atomic.acquireLock(pseudoModel.connection.db, doc.collection.name,
                           query, update, sync.defer()); sync.await();
        self._docs.push(doc);
    }, callback);
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
TransactionSchema.methods.removeDoc = function(doc, callback) {
    var pseudoModel;
    try {
        pseudoModel = ModelMap.getPseudoModel(doc);
    } catch(e) {
        return callback(e);
    }
    doc.isRemove = true;
    this.add(doc, callback);
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
TransactionSchema.methods._moveState = function(prev, delta, callback) {
    var self = this;
    callback = callback || function() {};
    var transactionModel;
    try {
        transactionModel = ModelMap.getPseudoModel(
            TransactionSchema.RAW_TRANSACTION_COLLECTION
        );
    } catch(e) {
        return callback(e);
    }
    var query = {_id: self._id, state: prev};
    TransactionSchema.attachShardKey(query);
    sync.fiber(function() {
        atomic.pseudoFindAndModify(transactionModel.connection.db,
                                   TransactionSchema.RAW_TRANSACTION_COLLECTION,
                                   query, delta,
                                   sync.defers('numberUpdated', 'collection'));
        var __ = sync.await();
        if (__.numberUpdated == 1) {
            return;
        }
        // if `findAndModify` return wrong result,
        // it only can wrong query case.
        var modQuery = utils.unwrapMongoOp(utils.wrapMongoOp(query));
        delete modQuery.state;

        __.collection.findOne(modQuery, sync.defer());
        var updatedDoc = sync.await();
        var state1 = delta.state || ((delta.$set || {}).state);
        var state2 = updatedDoc && updatedDoc.state;
        if (state1 != state2) {
            throw new TransactionError(ERROR_TYPE.SOMETHING_WRONG,
                                       {transaction: self, query: query});
        }
        return (updatedDoc && updatedDoc.history) || [];
    }, callback);
};

// ### Transaction.commit
// Commit all changes
//
// #### Arguments
// * callback - :Function:
//
// #### Callback arguments
// * err
TransactionSchema.methods.commit = function (callback) {
    var self = this;
    callback = callback || function() {};

    sync.fiber(function() {
        self._commit(sync.defer()); sync.await();
        var errors = [];
        async.each(self.history, function (history, next) {
            var pseudoModel;
            try {
                pseudoModel = ModelMap.getPseudoModel(history.col);
            } catch(e) {
                errors.push(e);
                return next();
            }
            var query = {_id: history.id, t: self._id};
            utils.addShardKeyDatas(pseudoModel, history, query);
            if (history.options && history.options.remove) {
                pseudoModel.connection.collection(history.col)
                           .remove(query, function(err) {
                    if (err) {
                        errors.push(err);
                    }
                    next();
                });
                return;
            }
            if (!history.op) {
                utils.nextTick(function () {
                    next();
                });
                return;
            }
            var op = utils.unwrapMongoOp(JSON.parse(history.op));
            utils.removeShardKeySetData(pseudoModel.shardKey, op);
            atomic.releaseLock(pseudoModel.connection.db, history.col, query,
                               op, function(err) {
                if (err) {
                    errors.push(err);
                }
                return next();
            });
        }, sync.defer()); sync.await();
        if (errors.length) {
            console.error('errors', errors);
            // TODO: cleanup batch
            return;
        }
        self.collection.remove({_id: self._id}, sync.defer()); sync.await();
    }, callback);
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
TransactionSchema.methods._makeHistory = function(callback) {
    var self = this;
    var errors = [];
    if (self.history.length) {
        return callback(errors);
    }
    async.each(self._docs || [], function(doc, next) {
        doc.validate(function(err) {
            var history = {
                col: doc.collection.name,
                id: doc._id,
                options: {
                    new: doc.isNew,
                    remove: doc.isRemove,
                },
            };
            try {
                utils.addShardKeyDatas(ModelMap.getPseudoModel(doc), doc,
                                       history);
            } catch(e) {
                return next(e);
            }
            self.history.push(history);
            if (err) {
                errors.push(err);
                return next(); // no need more action
            }

            var delta = utils.extractDelta(doc);
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
            next();
        });
    }, function() {
        callback(errors);
    });
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
TransactionSchema.methods._commit = function(callback) {
    var self = this;
    callback = callback || function() {};

    if (self.state == 'commit') {
        DEBUG('retry transaction commit', self._id);
        return callback();
    }

    if (self.state == 'expire') {
        DEBUG('transaction expired', self._id);
        return callback(
            new TransactionError(ERROR_TYPE.TRANSACTION_EXPIRED,
                                 {transaction: self})
        );
    }

    sync.fiber(function() {
        try {
            self._makeHistory(sync.defer()); sync.await();
        } catch(errors) {
            if (errors.length) {
                console.error(errors);
                self.expire(sync.defer()); sync.await();
                if (errors[0]) {
                    throw errors[0];
                }
                throw new TransactionError(
                    ERROR_TYPE.UNKNOWN_COMMIT_ERROR,
                    {transaction: self}
                );
            }
        }
        if (self.isExpired()) {
            self.expire(sync.defer()); sync.await();
            throw new TransactionError(ERROR_TYPE.TRANSACTION_EXPIRED,
                                       {transaction: self});
        }
        self.state = 'commit';
        DEBUG('transaction commit', self._id);
        var delta = utils.extractDelta(self);
        var history;
        try {
            self._moveState('pending', delta, sync.defer());
            history = sync.await();
        } catch(e) {
            // this case only can db error or already expired
            self.state = undefined;
            self.expire(sync.defer()); sync.await();
            throw new TransactionError(ERROR_TYPE.UNKNOWN_COMMIT_ERROR,
                                       {transaction: self});
        }
        if (!history) {
            self._docs.forEach(function (doc) {
                doc.emit('transactionAdded', self);
            });
        } else {
            self.history = history;
        }
        self.$__reset();
    }, callback);
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
TransactionSchema.methods._expire = function (callback) {
    var self = this;
    callback = callback || function() {};

    if (self.state == 'expire') {
        DEBUG('retry transaction expired', self._id);
        return callback();
    }
    if (self.state == 'commit') {
        DEBUG('transaction committed', self._id);
        return callback();
    }
    DEBUG('transaction expired', self._id);
    sync.fiber(function() {
        try {
            self._makeHistory(sync.defer()); sync.await();
        } catch(errors) {}
        self.state = 'expire';
        var delta = utils.extractDelta(self);
        self._moveState('pending', delta, sync.defer());
        var history = sync.await();
        if (history) {
            self.history = history;
        }
        self.$__reset();
    }, callback);
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
TransactionSchema.methods.expire = function(callback) {
    var self = this;
    callback = callback || function() {};

    sync.fiber(function() {
        self._expire(sync.defer()); sync.await();
        var errors = [];
        async.each(self.history, function (history, next) {
            if (!history.op) {
                return utils.nextTick(function () { next(); });
            }
            var pseudoModel;
            try {
                pseudoModel = ModelMap.getPseudoModel(history.col);
            } catch(e) {
                errors.push(e);
                return next();
            }
            var query = {_id: history.id, t: self._id};
            utils.addShardKeyDatas(pseudoModel, history, query);
            if (history.options && history.options.new) {
                pseudoModel.connection.collection(history.col)
                           .remove(query, function(err) {
                    if (err) {
                        errors.push(err);
                    }
                    next();
                });
                return;
            }
            atomic.releaseLock(pseudoModel.connection.db, history.col, query,
                               {$set: {t: DEFINE.NULL_OBJECTID}},
                               function(err) {
                if (err) {
                    errors.push(err);
                }
                return next();
            });
        }, sync.defer()); sync.await();
        if (errors.length) {
            console.error('errors', errors);
            // TODO: cleanup batch
            return;
        }
        self.collection.remove({_id: self._id}, sync.defer()); sync.await();
    }, callback);
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
TransactionSchema.methods.cancel = function(reason, callback) {
    callback = callback || function() {};
    this.expire(function() {
        callback(reason);
    });
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
TransactionSchema.methods._postProcess = function(callback) {
    switch (this.state) {
        case 'pending':
            if (this.isExpired()) {
                return this.expire(callback);
            }
            var hint = {collection: ModelMap.getCollectionName(this),
                        transaction: this};
            var error = new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2,
                                             hint);
            return callback(error);
        case 'commit':
            return this.commit(callback);
        case 'expire':
            return this.expire(callback);
        default:
            return callback();
    }
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
TransactionSchema.methods.find = function(model, conditions, options,
                                          callback) {
    var self = this;
    if (typeof(conditions) == 'function') {
        callback = conditions;
        conditions = {};
        options = {};
    }
    if (typeof(options) == 'function') {
        callback = options;
        options = {};
    }
    callback = callback || function() {};
    conditions = conditions || {};
    conditions.$or = conditions.$or || [];
    conditions.$or = conditions.$or.concat([{t: DEFINE.NULL_OBJECTID},
                                            {t: {$exists: false}}]);

    var pseudoModel;
    try {
        pseudoModel = ModelMap.getPseudoModel(model);
    } catch(e) {
        return callback(e);
    }

    sync.fiber(function() {
        model.collection.find(conditions, options, sync.defer());
        var cursor = sync.await();
        cursor.toArray(sync.defer());
        var docs = sync.await();
        if (!docs) {
            return;
        }
        async.map(docs, function(_doc, next) {
            var query = {_id: _doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                              {t: {$exists: false}}]};
            utils.addShardKeyDatas(pseudoModel, _doc, query);

            var query2 = {_id: _doc._id};
            utils.addShardKeyDatas(pseudoModel, _doc, query2);

            sync.fiber(function() {
                var remainRetry = 5;
                while (true) {
                    remainRetry -= 1;
                    var doc;
                    try {
                        model.findOneAndUpdate(query, {$set: {t: self._id}},
                                               {new: true,
                                                fields: options.fields},
                                               sync.defer());
                        doc = sync.await();
                        if (doc) {
                            self._docs.push(doc);
                            return doc;
                        }
                        if (!remainRetry) {
                            var hint = {
                                collection: _doc &&
                                            ModelMap.getCollectionName(_doc),
                                doc: _doc && _doc._id,
                                query: query,
                                transaction: self
                            };
                            throw new TransactionError(
                                ERROR_TYPE.TRANSACTION_CONFLICT_2,
                                hint
                            );
                        }
                        try {
                            model.findOne(query2, sync.defer()); sync.await();
                        } catch(e) {}
                    } catch(e) {
                        if (!remainRetry) {
                            throw e;
                        }
                    }
                    setTimeout(sync.defer(), _.sample([37, 59, 139]));
                    sync.await();
                }
            }, next);
        }, sync.defer());
        return sync.await();
    }, callback);
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
TransactionSchema.methods.findOne = function(model, conditions, options,
                                             callback) {
    var self = this;
    if (typeof(conditions) == 'function') {
        callback = conditions;
        conditions = {};
        options = {};
    }
    if (typeof(options) == 'function') {
        callback = options;
        options = {};
    }
    callback = callback || function() {};
    conditions = conditions || {};

    var pseudoModel;
    try {
        pseudoModel = ModelMap.getPseudoModel(model);
    } catch(e) {
        return callback(e);
    }

    sync.fiber(function() {
        model.collection.findOne(conditions, options, sync.defer());
        var _doc = sync.await();
        if (!_doc) {
            return;
        }
        var query1 = {_id: _doc._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                          {t: {$exists: false}}]};
        utils.addShardKeyDatas(pseudoModel, _doc, query1);
        var query2 = {_id: _doc._id};
        utils.addShardKeyDatas(pseudoModel, _doc, query2);
        var remainRetry = 5;
        while (true) {
            remainRetry -= 1;
            var doc;
            try {
                model.findOneAndUpdate(query1, {$set: {t: self._id}},
                                       {new: true, fields: options.fields},
                                       sync.defer());
                doc = sync.await();
            } catch(e) {
                if (!remainRetry) {
                    throw e;
                }
            }
            if (doc) {
                self._docs.push(doc);
                return doc;
            }
            if (!remainRetry) {
                throw new TransactionError(
                    ERROR_TYPE.TRANSACTION_CONFLICT_2,
                    {collection: _doc && ModelMap.getCollectionName(_doc),
                     doc: _doc && _doc._id, query: query1, transaction: self}
                );
            }
            // if t is not NULL_OBJECTID, try to go end of transaction process
            try {
                model.findOne(query2, sync.defer()); sync.await();
            } catch(e) {}
            setTimeout(sync.defer(), _.sample([37, 59, 139])); sync.await();
        }
    }, callback);
};

module.exports = TransactionSchema;
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
