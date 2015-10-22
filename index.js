// mongoose-transaction
// ===========
// `mongoose-transaction` provide transaction feature under multiple mongodb
// connections/documents.
//
// ## State
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
// ## Legacy
// this project has some legacy codes. These codes need to care
// mongo/mongoose's buggy actions
//
// ### findAndModify
// Transaciton require `mongo native atomic` actions, so we only can use
// `findAndModify` or `update` command.
//
// but `findAndModify` command will lock of all shard collections,
// make `pseudo findAndModify` using combine two commands(`update`, `findOne`)
//
// ### update
// Some `specific` mongodb's `update` command return buggy result.
//
// - data write finished, `update` return updated documents count is 0.
// - mongoose use this count, it raise error
//
// So, we decide if update result count is 0, send `findOne` command,
// recheck document change.
"use strict";
var util = require('util');
var sync = require('synchronize');
var mongoose = require('mongoose');
var mongooseutils = require('mongoose/lib/utils');
var async = require('async');
var _ = require('underscore');
var utils = require('./utils');

module.exports = {};

var DEBUG = function() {
    if (global.TRANSACTION_DEBUG_LOG) {
        console.log.apply(console, arguments);
    }
};

var wrapMongoOp = utils.wrapMongoOp;
var unwrapMongoOp = utils.unwrapMongoOp;

var CollectionPseudoModelMap = {};

var MONGOOSE_VERSIONS = mongoose.version.split('.').map(function(x) {
    return parseInt(x, 10 );
});

//var VERSION_TRANSACTION = 4;
var TRANSACTION_COLLECTION = module.exports.TRANSACTION_COLLECTION =
        'Transaction';
var RAW_TRANSACTION_COLLECTION =
        mongooseutils.toCollectionName(TRANSACTION_COLLECTION);

var ONE_MINUTE = 60 * 1000;
var TRANSACTION_EXPIRE_GAP = module.exports.TRANSACTION_EXPIRE_GAP =
        ONE_MINUTE;

var NULL_OBJECTID = mongoose.Types.ObjectId("000000000000000000000000");
module.exports.NULL_OBJECTID = NULL_OBJECTID;

var TransactionError = module.exports.TransactionError =
        function TransactionError(type, document, transaction, delta, vd) {
    Error.call(this);
    Error.captureStackTrace(this, TransactionError);
    this.name = 'TransactionError';
    this.message = this.type = type || 'unknown';
    var collectionName =
            document && document.collection && document.collection.name;
    var docId = document && document._id;
    this.hint = [collectionName, docId, transaction, delta, vd];
};
util.inherits(module.exports.TransactionError, Error);

var TRANSACTION_ERRORS = module.exports.TRANSACTION_ERRORS = {
    BROKEN_DATA: 40,
    SOMETHING_WRONG: 41, // data not found or mongo response error
    TRANSACTION_CONFLICT_1: 42, // sequence save
    TRANSACTION_CONFLICT_2: 43, // transacted lock
    TRANSACTION_EXPIRED: 44,
    COMMON_ERROR_RETRY: 45,
    JUST_RETRY: 46,
    INVALID_COLLECTION: 50,
    UNKNOWN_COMMIT_ERROR: 60,
    INFINITE_LOOP: 70
};

// TODO: update expire time
var TransactionSchema = new mongoose.Schema({
    history: [],
    state: { type: String, required: true, 'default': 'pending' },
});

// exports
module.exports.TransactionSchema = TransactionSchema;
// FIXME: rename this plugin
module.exports.bindShardKeyRule = function(schema, rule) {
    if (!rule || !rule.fields || !rule.rule || !rule.initialize ||
            !Object.keys(rule.rule).length ||
            typeof(rule.initialize) !== 'function') {
        throw new TransactionError(TRANSACTION_ERRORS.BROKEN_DATA);
    }
    schema.add(rule.fields);
    schema.options.shardKey = rule.rule;
    schema.options.shardKey.initialize = rule.initialize;
};

var addShardKeyDatas = function(pseudoModel, src, dest) {
    if (!pseudoModel || !pseudoModel.shardKey ||
            !Array.isArray(pseudoModel.shardKey)) {
        return;
    }
    pseudoModel.shardKey.forEach(function(sk) { dest[sk] = src[sk]; });
};
var removeShardKeySetData = function(shardKey, op) {
    if (!shardKey || !Array.isArray(shardKey)) {
        return;
    }
    if (!op.$set) {
        return;
    }
    shardKey.forEach(function(sk) {
        delete op.$set[sk];
    });
};
var getShardKeyArray = function(shardKey) {
    return Array.isArray(shardKey) ? shardKey : [];
};
var _getPseudoModel = function(model) {
    if (!model) {
        throw new TransactionError(TRANSACTION_ERRORS.INVALID_COLLECTION);
    }
    var key;
    if (model.collection && model.collection.name) {
        key = model.collection.name;
    } else {
        key = model;
    }
    var pseudoModel = CollectionPseudoModelMap[key];
    if (!pseudoModel) {
        throw new TransactionError(TRANSACTION_ERRORS.INVALID_COLLECTION);
    }
    return pseudoModel;
};
var _attachShardKey = function(doc) {
    if (!TransactionSchema.options.shardKey ||
            !TransactionSchema.options.shardKey.initialize) {
        return;
    }
    TransactionSchema.options.shardKey.initialize(doc);
};

// ## PseudoFindAndModify
// These functions combine `update` and `find`,
// emulate `findAndModify` of mongodb
var _pseudoFindAndModify = function(db, collectionName, query, update,
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

// ### pseudoFindAndModify1
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
var pseudoFindAndModify1 = function(db, collectionName, query, update,
                                    callback) {
    callback = callback || function() {};
    sync.fiber(function() {
        _pseudoFindAndModify(db, collectionName, query, update,
                             sync.defers('numberUpdated', 'collection'));
        var __ = sync.await();
        if (__.numberUpdated == 1) {
            return;
        }
        var modQuery = unwrapMongoOp(wrapMongoOp(query));
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
        var virtualDoc = {
            collection: {name: collectionName},
            _id: query._id
        };
        if (!updatedDoc) {
            throw new TransactionError(TRANSACTION_ERRORS.SOMETHING_WRONG,
                                       virtualDoc);
        }
        throw new TransactionError(TRANSACTION_ERRORS.TRANSACTION_CONFLICT_1,
                                   virtualDoc);
    }, callback);
};

// ### pseudoFindAndModify2
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
var pseudoFindAndModify2 = function(db, collectionName, query, update,
                                    callback) {
    callback = callback || function() {};
    sync.fiber(function() {
        _pseudoFindAndModify(db, collectionName, query, update,
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
    var data;
    sync.fiber(function() {
        // below 3.6.x
        if (MONGOOSE_VERSIONS[0] < 3 ||
                (MONGOOSE_VERSIONS[0] == 3 && MONGOOSE_VERSIONS[1] <= 6)) {
            connection.db.executeDbCommand({findAndModify: collection.name,
                                            query: query,
                                            update: update,
                                            fields: fields,
                                            new: true},
                                            sync.defer());
            data = sync.await();
            if (!data || !data.documents || !data.documents[0]) {
                throw new TransactionError(TRANSACTION_ERRORS.SOMETHING_WRONG,
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
        if (MONGOOSE_VERSIONS[0] < 4) {
            return data;
        }
        if (!data) {
            throw new TransactionError(TRANSACTION_ERRORS.SOMETHING_WRONG,
                                       {collection: collection.name,
                                        query: query, update: update});
        }
        // above 4.x
        return data.value;
    }, callback);
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
    return TRANSACTION_EXPIRE_GAP < diff;
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
    _attachShardKey(this);
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
// SeeAlso :pseudoFindAndModify1:
TransactionSchema.methods.add = function (doc, callback) {
    callback = callback || function() {};
    var self = this;
    if (Array.isArray(doc)) {
        return async.each(doc, function(d, next) {
            self.add(d, next);
        }, callback);
    }
    var pseudoModel = _getPseudoModel(doc);
    _attachShardKey(self);
    sync.fiber(function() {
        doc.validate(sync.defer()); sync.await();
        if (doc.isNew) {
            // create new document
            var data = {_id: doc._id, t: self._id, __new: true};
            addShardKeyDatas(pseudoModel, doc, data);
            doc.collection.insert(data, sync.defer()); sync.await();
            self._docs.push(doc);
            return;
        }
        if (doc.t && !NULL_OBJECTID.equals(doc.t) &&
                doc.t.toString() != self._id.toString()) {
            throw new TransactionError(TRANSACTION_ERRORS.BROKEN_DATA, doc,
                                       self);
        }
        if (doc.t && !NULL_OBJECTID.equals(doc.t)) {
            // FIXME: preload transacted
            return;
        }
        var query = {_id: doc._id, $or: [{t: NULL_OBJECTID},
            {t: {$exists: false}}]};
        addShardKeyDatas(pseudoModel, doc, query);
        var update = {$set: {t: self._id}};
        pseudoFindAndModify1(pseudoModel.connection.db, doc.collection.name,
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
        pseudoModel = _getPseudoModel(doc);
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
        transactionModel = _getPseudoModel(RAW_TRANSACTION_COLLECTION);
    } catch(e) {
        return callback(e);
    }
    var query = {_id: self._id, state: prev};
    _attachShardKey(query);
    sync.fiber(function() {
        _pseudoFindAndModify(transactionModel.connection.db,
                             RAW_TRANSACTION_COLLECTION, query, delta,
                             sync.defers('numberUpdated', 'collection'));
        var __ = sync.await();
        if (__.numberUpdated == 1) {
            return;
        }
        // if `findAndModify` return wrong result,
        // it only can wrong query case.
        var modQuery = unwrapMongoOp(wrapMongoOp(query));
        delete modQuery.state;

        __.collection.findOne(modQuery, sync.defer());
        var updatedDoc = sync.await();
        var state1 = delta.state || ((delta.$set || {}).state);
        var state2 = updatedDoc && updatedDoc.state;
        if (state1 != state2) {
            throw new TransactionError(TRANSACTION_ERRORS.SOMETHING_WRONG);
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
                pseudoModel = _getPseudoModel(history.col);
            } catch(e) {
                errors.push(e);
                return next();
            }
            var query = {_id: history.id, t: self._id};
            addShardKeyDatas(pseudoModel, history, query);
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
            var op = unwrapMongoOp(JSON.parse(history.op));
            removeShardKeySetData(pseudoModel, op);
            pseudoFindAndModify2(pseudoModel.connection.db, history.col, query,
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
                addShardKeyDatas(_getPseudoModel(doc), doc, history);
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
            delta.$set.t = NULL_OBJECTID;

            if (doc.isNew) {
                delta.$set = doc.toObject({depopulate: 1});
                if (!doc.$__.version) {
                    doc.$__version(true, delta.$set);
                }
                delete delta.$set._id;
                delta.$unset = {__new: 1};
            }

            DEBUG('DELTA', doc.collection.name, ':', JSON.stringify(delta));
            history.op = delta && JSON.stringify(wrapMongoOp(delta));

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
            new TransactionError(TRANSACTION_ERRORS.TRANSACTION_EXPIRED, null,
                                 self)
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
                    TRANSACTION_ERRORS.UNKNOWN_COMMIT_ERROR, null, self
                );
            }
        }
        if (self.isExpired()) {
            self.expire(sync.defer()); sync.await();
            throw new TransactionError(TRANSACTION_ERRORS.TRANSACTION_EXPIRED,
                                       null, self);
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
            throw new TransactionError(TRANSACTION_ERRORS.UNKNOWN_COMMIT_ERROR,
                                       null, self);
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
                pseudoModel = _getPseudoModel(history.col);
            } catch(e) {
                errors.push(e);
                return next();
            }
            var query = {_id: history.id, t: self._id};
            addShardKeyDatas(pseudoModel, history, query);
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
            pseudoFindAndModify2(pseudoModel.connection.db, history.col, query,
                                 {$set: {t: NULL_OBJECTID}},
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
            return callback(
                new TransactionError(TRANSACTION_ERRORS.TRANSACTION_CONFLICT_2,
                                     this)
            );
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
    conditions.$or = conditions.$or.concat([{t: NULL_OBJECTID},
                                            {t: {$exists: false}}]);
    return conditions;
};

// ### Transaction.find
// Find documents with assgined `t`
//
// TODO: rename like `findAndAttachT`
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
    conditions.$or = conditions.$or.concat([{t: NULL_OBJECTID},
                                            {t: {$exists: false}}]);
    var pseudoModel;
    try {
        pseudoModel = _getPseudoModel(model);
    } catch(e) {
        return callback(e);
    }

    sync.fiber(function() {
        // FIXME reduce db query
        // - define select fields; it only need `_id`
        // - maybe find query should'nt contains `t: NULL_OBJECTID`
        model.collection.find(conditions, options, sync.defer());
        var cursor = sync.await();
        cursor.toArray(sync.defer());
        var docs = sync.await();
        if (!docs) {
            return;
        }
        async.map(docs, function(_doc, next) {
            var query = {_id: _doc._id, $or: [{t: NULL_OBJECTID},
                                              {t: {$exists: false}}]};
            addShardKeyDatas(pseudoModel, _doc, query);
            model.findOneAndUpdate(query, {$set: {t: self._id}},
                                   {new: true, fields: options.fields},
                                   function(err, doc) {
                if (err) {
                    return next(err);
                }
                if (!doc) {
                    return next(
                        new TransactionError(
                            TRANSACTION_ERRORS.TRANSACTION_CONFLICT_2,
                            _doc, self
                        )
                    );
                }
                self._docs.push(doc);
                next(null, doc);
            });
        }, sync.defer());
        return sync.await();
    }, callback);
};

// ### Transaction.findWithWaitRetry
// Find documents with assgined `t`
//
// If conflict another process, this method wait and retry action
//
// FIXME: merge to :Transaction.find:
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
// SeeAlso :Transaction.find:
TransactionSchema.methods.findWithWaitRetry = function(model, conditions,
                                                       options, callback) {
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
    conditions.$or = conditions.$or.concat([{t: NULL_OBJECTID},
                                            {t: {$exists: false}}]);

    var pseudoModel;
    try {
        pseudoModel = _getPseudoModel(model);
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
            var query = {_id: _doc._id, $or: [{t: NULL_OBJECTID},
                                              {t: {$exists: false}}]};
            addShardKeyDatas(pseudoModel, _doc, query);

            var query2 = {_id: _doc._id};
            addShardKeyDatas(pseudoModel, _doc, query2);

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
                            throw new TransactionError(
                                TRANSACTION_ERRORS.TRANSACTION_CONFLICT_2,
                                _doc, self
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
// Find one documents with assgined `t`
//
// TODO: rename like `findOneAndAttachT`
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
        pseudoModel = _getPseudoModel(model);
    } catch(e) {
        return callback(e);
    }

    // TODO: combine one query
    sync.fiber(function() {
        model.collection.findOne(conditions, options, sync.defer());
        var _doc = sync.await();
        if (!_doc) {
            return;
        }
        var query = {_id: _doc._id, $or: [{t: NULL_OBJECTID},
                                          {t: {$exists: false}}]};
        addShardKeyDatas(pseudoModel, _doc, query);
        model.findOneAndUpdate(query, {$set: {t: self._id}},
                               {new: true, fields: options.fields},
                               sync.defer());
        var doc = sync.await();
        if (!doc) {
            throw new TransactionError(
                TRANSACTION_ERRORS.TRANSACTION_CONFLICT_2, _doc, self
            );
        }
        self._docs.push(doc);
        return doc;
    }, callback);
};

// ### Transaction.findOneWithWaitRetry
// Find documents with assgined `t`
//
// If conflict another process, this method wait and retry action
//
// FIXME: merge to :Transaction.findOne:
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
// SeeAlso :Transaction.findOne:
TransactionSchema.methods.findOneWithWaitRetry = function(model, conditions,
                                                          options, callback) {
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
        pseudoModel = _getPseudoModel(model);
    } catch(e) {
        return callback(e);
    }

    sync.fiber(function() {
        model.collection.findOne(conditions, options, sync.defer());
        var _doc = sync.await();
        if (!_doc) {
            return;
        }
        var query1 = {_id: _doc._id, $or: [{t: NULL_OBJECTID},
                                          {t: {$exists: false}}]};
        addShardKeyDatas(pseudoModel, _doc, query1);
        var query2 = {_id: _doc._id};
        addShardKeyDatas(pseudoModel, _doc, query2);
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
                    TRANSACTION_ERRORS.TRANSACTION_CONFLICT_2,
                    _doc, self
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


// ### TransactedModel._saveWithoutTransaction
// This method replace original `Model.save`
//
// Save action only can succeed document`s t value is NULL_OBJECTID
//
// mongoose version
// - below 3.x replace save
// - above 4.x set `save prehook`
//
// #### Arguments
// * next - :Function: this chain call original save
// * callback - :Function:
//
// #### Callback arguments
// * err
//
// #### Transaction errors
// * 41 - cannot found update target document
// * 42 - conflict another transaction; document locked
// * 43 - conflict another transaction; transaction still alive
var saveWithoutTransaction = function(next, callback) {
    var self = this;
    callback = callback || function() {};

    if (self.isNew) {
        return next();
    }
    sync.fiber(function() {
        self.validate(sync.defer()); sync.await();
        var delta = self.$__delta();
        if (!delta) {
            return;
        }
        delta = delta[1];
        DEBUG('DELTA', self.collection.name, ':', JSON.stringify(delta));
        var pseudoModel = _getPseudoModel(self);
        self.$__reset();
        // manually save document only can `t` value is unset or NULL_OBJECTID
        var query = {_id: self._id, $or:[{t: NULL_OBJECTID},
                                         {t: {$exists: false}}]};
        addShardKeyDatas(pseudoModel, self, query);
        // TODO: need delta cross check
        var checkFields = {t: 1};
        findAndModifyMongoNative(pseudoModel.connection, self.collection,
                                 query, delta, checkFields,
                                 sync.defer());
        var data = sync.await();
        if (!data) {
            throw new TransactionError(
                TRANSACTION_ERRORS.TRANSACTION_CONFLICT_1, self
            );
        }
        if (data.t && !NULL_OBJECTID.equals(data.t)) {
            throw new TransactionError(
                TRANSACTION_ERRORS.TRANSACTION_CONFLICT_2, self
            );
        }
        self.emit('transactedSave');
    }, callback);
};

module.exports.plugin = function(schema) {
    schema.add({t: {type: mongoose.Schema.Types.ObjectId,
                    'default': NULL_OBJECTID}});
    schema.add({__new: Boolean});

    if (MONGOOSE_VERSIONS[0] < 4) {
        schema.post('init', function() {
            var self = this;
            self._oldSave = self.save;
            self.save = function(callback) {
                self._saveWithoutTransaction(function(err) {
                    if (err) {
                        return callback(err);
                    }
                    self._oldSave(callback);
                }, callback);
            };
        });
    } else {
        schema.pre('save', function(next, callback) {
            this._saveWithoutTransaction(next, callback);
        });
    }

    schema.methods._saveWithoutTransaction = saveWithoutTransaction;
};

// ## TransactedModel
// TransactedModel will expand or replace mongoose model's query commands.
//
// It also replace `connection.model('Test', TestSchema)`
//
// - all original method mean change;
//   if document's `t` value is not NULL_OBJECTID, will raise transaction error
// - add shortcuts of native query commands; `findNative`, `findOneNative`
//
// | orig method        | retry query                | ignore lock        |
// |--------------------|----------------------------|--------------------|
// | find               | findWithWaitRetry          | findForce          |
// | findOne            | findOneWithWaitRetry       | findOneForce       |
// | findById           | findByIdWithWaitRetry      | findByIdForce      |
// | collection.find    | findNativeWithWaitRetry    | findNativeForce    |
// | collection.findOne | findOneNativeWithWaitRetry | findOneNativeForce |
//
// :Constructor:
//
// #### Arguments
// * connection - :Connection:
// * modelName - :String:
// * schema - :Schema:
var addCollectionPseudoModelPair =
    module.exports.addCollectionPseudoModelPair =
        function(collectionName, connection, schema) {
    var shardKey;
    if (schema.options && schema.options.shardKey) {
        shardKey = Object.keys(schema.options.shardKey);
    }
    CollectionPseudoModelMap[collectionName] = {
        connection: connection,
        shardKey: shardKey
    };
};


var filterTransactedDocs = function(docs, callback) {
    callback = callback || function() {};
    if (!docs) {
        return callback(new Error('invalid documents'));
    }
    sync.fiber(function() {
        var transactionIdDocsMap = {};
        var transactionIds = [];
        var result = {ids: transactionIds,
                      map: transactionIdDocsMap};
        if (docs.nextObject) {
            var doc = true;
            while (doc) {
                docs.nextObject(sync.defer());
                doc = sync.await();
                if (!doc) {
                    break;
                }
                if (!doc.t || NULL_OBJECTID.equals(doc.t)) {
                    continue;
                }
                transactionIdDocsMap[doc.t] =
                        transactionIdDocsMap[doc.t] || [];
                transactionIdDocsMap[doc.t].push(doc);
                if (!_.contains(transactionIds, doc.t)) {
                    transactionIds.push(doc.t);
                }
            }
        }
        if (docs.forEach) {
            docs.forEach(function(doc) {
                if (!doc || !doc.t ||
                        NULL_OBJECTID.equals(doc.t)) {
                    return;
                }
                transactionIdDocsMap[doc.t] =
                    transactionIdDocsMap[doc.t] || [];
                transactionIdDocsMap[doc.t].push(doc);
                if (_.contains(transactionIds, doc.t)) {
                    return;
                }
                transactionIds.push(doc.t);
            });
        }
        return result;
    }, callback);
};

var recheckTransactions = function(model, transactedDocs, callback) {
    var transactionIds = transactedDocs.ids;
    var transactionIdDocsMap = transactedDocs.map;

    sync.fiber(function() {
        transactionIds.forEach(function(transactionId) {
            var query = {
                _id: transactionId,
            };
            _attachShardKey(query);
            var transactionModel = _getPseudoModel(RAW_TRANSACTION_COLLECTION);
            transactionModel.connection
                            .models[TRANSACTION_COLLECTION]
                            .findOne(query, sync.defer());
            var tr = sync.await();
            if (tr && tr.state != 'done') {
                tr._postProcess(sync.defer()); sync.await();
                return;
            }
            async.each(transactionIdDocsMap[transactionId],
                       function(doc, next) {
                var pseudoModel;
                try {
                    pseudoModel = _getPseudoModel(model);
                } catch(e) {
                    return next(e);
                }
                var query = {_id: doc._id, t: doc.t};
                addShardKeyDatas(pseudoModel, doc, query);
                if (doc.__new) {
                    return model.collection.remove(query,
                                                   next);
                }
                return pseudoFindAndModify2(
                    pseudoModel.connection.db,
                    model.collection.name,
                    query,
                    {$set: {t: NULL_OBJECTID}},
                    next
                );
            }, sync.defer()); sync.await();
        });
    }, callback);
};

// ### TransactedModel.find...
// Document during query, if conflict the another transaction,
// method will raise transaction error
//
// But, if document transaction is already finished,
// method will process post actions and retry find document
//
// #### Arguments
// SeeAlso :Model:
//
// #### Callback arguments
// * err
// * doc - :Document: or :Array:
//
// #### Transaction errors
// * 50 - unknown colllection
// * 70 - Exceed retry limit
// SeeAlso :Transaction._postProcess:
var find = function(proto) {
    return function() {
        var args = Array.prototype.slice.call(arguments);
        var callback = args[args.length - 1];

        if (!callback || typeof(callback) != 'function') {
            // using special case;
            //   `Model.find({}).sort({}).exec(function() {})`
            // FIXME: support this case
            throw new Error('TRANSACTION_FIND_NOT_SUPPORT_QUERY_CHAIN');
        }
        args.pop();
        if (args.length > 1) {
            var pseudoModel;
            try {
                pseudoModel = _getPseudoModel(proto.model);
            } catch(e) {
                return callback(e);
            }
            var _defaultFields =
                    ['t'].concat(getShardKeyArray(pseudoModel.shardKey));
            args[1] = utils.setDefaultFields(args[1], _defaultFields);
        }

        sync.fiber(function() {
            var RETRY_LIMIT = 10;
            var retry = RETRY_LIMIT;
            while (retry) {
                retry -= 1;
                proto.orig.apply(proto.target, args.concat(sync.defer()));
                var docs = sync.await();
                if (!proto.isMultiple) {
                    docs = [docs];
                }
                filterTransactedDocs(docs, sync.defer());
                var transactedDocs = sync.await();
                if (!transactedDocs.ids.length) {
                    // FIXME need return
                    docs = proto.isMultiple ? docs : docs[0];
                    if (docs && docs.rewind) {
                        docs.rewind();
                    }
                    return docs;
                }
                recheckTransactions(proto.model, transactedDocs, sync.defer());
                sync.await();
            }
            throw new TransactionError(TRANSACTION_ERRORS.INFINITE_LOOP);
        }, callback);
    };
};

// ### TransactedModel.find...Force
// Same action original method
//
// This method will ignore transaction lock
//
// #### Arguments
// SeeAlso :Model:
//
// #### Callback arguments
// * err
// * doc - :Document: or :Array:
var findForce = function(proto) {
    return function() {
        return proto.orig.apply(proto.target, arguments);
        // TODO delete save method
    };
};

// ### TransactedModel.find...WithWaitRetry
// Document during query, if conflict the another transaction,
// method will wait-and-retry a number of times.
//
// #### Arguments
// SeeAlso :Model:
//
// #### Callback arguments
// * err
// * doc - :Document: or :Array:
//
// #### Transaction errors
// SeeAlso :TransactedModel.find...:
var findWithWaitRetry = function(proto) {
    return function() {
        var args = Array.prototype.slice.call(arguments);
        var callback = args[args.length - 1];

        if (!callback || typeof(callback) != 'function') {
            // using special case;
            //   `Model.find({}).sort({}).exec(function() {})`
            // FIXME: support this case
            throw new Error('TRANSACTION_FIND_NOT_SUPPORT_QUERY_CHAIN');
        }
        args.pop();
        if (args.length > 1) {
            var pseudoModel;
            try {
                pseudoModel = _getPseudoModel(proto.model);
            } catch(e) {
                return callback(e);
            }
            var _defaultFields =
                    ['t'].concat(getShardKeyArray(pseudoModel.shardKey));
            args[1] = utils.setDefaultFields(args[1], _defaultFields);
        }

        sync.fiber(function() {
            var remainRetry = 5;
            while (true) {
                remainRetry -= 1;
                var docs;
                try {
                    find(proto).apply(proto.model,
                                      args.concat(sync.defer()));
                    docs = sync.await();
                    return docs;
                } catch(e) {
                    if (!remainRetry) {
                        throw e;
                    }
                }
                setTimeout(sync.defer(), _.sample([37, 59, 139]));
                sync.await();
            }
        }, callback);
    };
};

module.exports.TransactedModel = function(connection, modelName, schema) {
    schema.plugin(module.exports.plugin);
    var model = connection.model(modelName, schema);
    addCollectionPseudoModelPair(model.collection.name, connection, schema);

    var toJSON = model.prototype.toJSON;
    model.prototype.toJSON = function transactedModelToJSON(options) {
        var res = toJSON.call(this, options);
        if (res.t && NULL_OBJECTID.equals(res.t)) {
            delete res.t;
        }
        return res;
    };

    var prototypes = [
        {
            target: model,
            name: 'find',
            alternative: null,
            isMultiple: true,
        },
        {
            target: model,
            name: 'findOne',
            alternative: null,
            isMultiple: false,
        },
        {
            target: model.collection,
            name: 'find',
            alternative: 'findNative',
            isMultiple: true,
        },
        {
            target: model.collection,
            name: 'findOne',
            alternative: 'findOneNative',
            isMultiple: false,
        },
    ];
    prototypes.forEach(function(proto) {
        var methodName = proto.alternative || proto.name;
        proto.model = model;
        proto.orig = proto.target[proto.name];
        // FIXME
        proto.target[proto.name + 'Force'] = proto.orig;
        model[methodName] = find(proto);
        model[methodName + 'Force'] = findForce(proto);
        model[methodName + 'WithWaitRetry'] =
                proto.target[proto.name + 'WithWaitRetry'] =
                findWithWaitRetry(proto);
    });

    // syntactic sugar
    ['', 'Force', 'WithWaitRetry'].forEach(function (lock) {
        model['findById' + lock] = function () {
            var args = Array.prototype.slice.call(arguments);
            args[0] = {_id: args[0]};
            return this['findOne' + lock].apply(this, args);
        };
    });

    return model;
};
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
