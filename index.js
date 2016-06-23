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
var async = require('async');
var _ = require('underscore');
var utils = require('./utils');
var atomic = require('./atomic');
var TransactionError = require('./error');
var DEFINE = require('./define');
var TransactionSchema = require('./schema');
var ModelMap = require('./modelmap');
var ERROR_TYPE = DEFINE.ERROR_TYPE;

module.exports = {
    TRANSACTION_ERRORS: ERROR_TYPE,
    TransactionError: TransactionError,
};

var DEBUG = utils.DEBUG;


//var VERSION_TRANSACTION = 4;
module.exports.TRANSACTION_COLLECTION =
    TransactionSchema.TRANSACTION_COLLECTION;

module.exports.TRANSACTION_EXPIRE_GAP =
    TransactionSchema.TRANSACTION_EXPIRE_GAP;

module.exports.NULL_OBJECTID = DEFINE.NULL_OBJECTID;

// exports
module.exports.TransactionSchema = TransactionSchema;
// FIXME: rename this plugin
module.exports.bindShardKeyRule = function(schema, rule) {
    if (!rule || !rule.fields || !rule.rule || !rule.initialize ||
            !Object.keys(rule.rule).length ||
            typeof(rule.initialize) !== 'function') {
        throw new TransactionError(ERROR_TYPE.BROKEN_DATA);
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
        var pseudoModel = ModelMap.getPseudoModel(self);
        self.$__reset();
        // manually save document only can `t` value is unset or NULL_OBJECTID
        var query = {_id: self._id, $or:[{t: DEFINE.NULL_OBJECTID},
                                         {t: {$exists: false}}]};
        addShardKeyDatas(pseudoModel, self, query);
        // TODO: need delta cross check
        var checkFields = {t: 1};
        atomic.findAndModify(pseudoModel.connection, self.collection,
                             query, delta, checkFields, sync.defer());
        var data = sync.await();
        if (!data) {
            throw new TransactionError(
                ERROR_TYPE.TRANSACTION_CONFLICT_1,
                {collection: ModelMap.getCollectionName(self), doc: self._id,
                 query: query}
            );
        }
        if (data.t && !DEFINE.NULL_OBJECTID.equals(data.t)) {
            throw new TransactionError(
                ERROR_TYPE.TRANSACTION_CONFLICT_2,
                {collection: ModelMap.getCollectionName(self), doc: self._id,
                 query: query}
            );
        }
        self.emit('transactedSave');
    }, callback);
};

module.exports.plugin = function(schema) {
    schema.add({t: {type: mongoose.Schema.Types.ObjectId,
                    'default': DEFINE.NULL_OBJECTID}});
    schema.add({__new: Boolean});

    if (DEFINE.MONGOOSE_VERSIONS[0] < 4) {
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
module.exports.addCollectionPseudoModelPair =
        ModelMap.addCollectionPseudoModelPair;

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
                if (!doc.t || DEFINE.NULL_OBJECTID.equals(doc.t)) {
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
                if (!doc || !doc.t || DEFINE.NULL_OBJECTID.equals(doc.t)) {
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
            TransactionSchema.attachShardKey(query);
            var transactionModel = ModelMap.getPseudoModel(
                TransactionSchema.RAW_TRANSACTION_COLLECTION
            );
            transactionModel.connection
                            .models[TransactionSchema.TRANSACTION_COLLECTION]
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
                    pseudoModel = ModelMap.getPseudoModel(model);
                } catch(e) {
                    return next(e);
                }
                var query = {_id: doc._id, t: doc.t};
                addShardKeyDatas(pseudoModel, doc, query);
                if (doc.__new) {
                    return model.collection.remove(query,
                                                   next);
                }
                var updateQuery = {$set: {t: DEFINE.NULL_OBJECTID}};
                return atomic.releaseLock(pseudoModel.connection.db,
                                          model.collection.name,
                                          query, updateQuery, next);
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
                pseudoModel = ModelMap.getPseudoModel(proto.model);
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
            throw new TransactionError(
                ERROR_TYPE.INFINITE_LOOP,
                {collection: ModelMap.getCollectionName(proto.model),
                 query: args.length > 1 && args[0] || {}}
            );
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

// ### TransactedModel.find...
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
var findWaitUnlock = function(proto) {
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
                pseudoModel = ModelMap.getPseudoModel(proto.model);
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
    ModelMap.addCollectionPseudoModelPair(model.collection.name, connection,
                                          schema);

    var toJSON = model.prototype.toJSON;
    model.prototype.toJSON = function transactedModelToJSON(options) {
        var res = toJSON.call(this, options);
        if (res.t && DEFINE.NULL_OBJECTID.equals(res.t)) {
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
        model[methodName] = findWaitUnlock(proto);
        model[methodName + 'Force'] = findForce(proto);
    });

    // syntactic sugar
    ['', 'Force'].forEach(function (lock) {
        model['findById' + lock] = function () {
            var args = Array.prototype.slice.call(arguments);
            args[0] = {_id: args[0]};
            return this['findOne' + lock].apply(this, args);
        };
    });

    return model;
};
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
