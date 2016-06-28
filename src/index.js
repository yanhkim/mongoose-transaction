// mongoose-transaction
// ===========
// `mongoose-transaction` provide transaction feature under multiple mongodb
// connections/documents.
//
'use strict';
const Promise = require('songbird');
const mongoose = require('mongoose');
const _ = require('underscore');
const utils = require('./utils');
const atomic = require('./atomic');
const TransactionError = require('./error');
const DEFINE = require('./define');
const TransactionSchema = require('./schema');
const ModelMap = require('./modelmap');
const helper = require('./helper');
const ERROR_TYPE = DEFINE.ERROR_TYPE;

module.exports = {
    TRANSACTION_ERRORS: ERROR_TYPE,
    TransactionError: TransactionError,
};

const DEBUG = utils.DEBUG;

// const VERSION_TRANSACTION = 4;
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
            typeof rule.initialize !== 'function') {
        throw new TransactionError(ERROR_TYPE.BROKEN_DATA);
    }
    schema.add(rule.fields);
    schema.options.shardKey = rule.rule;
    schema.options.shardKey.initialize = rule.initialize;
};

let getShardKeyArray = function(shardKey) {
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
const saveWithoutTransaction = async function(next, callback)  {
    let self = this;
    if (self.isNew) {
        return next();
    }

    let promise = (async() => {
        await helper.validate(self);
        let delta = self.$__delta();
        if (!delta) {
            return;
        }
        delta = delta[1];
        DEBUG('DELTA', self.collection.name, ':', JSON.stringify(delta));
        let pseudoModel = ModelMap.getPseudoModel(self);
        self.$__reset();
        // manually save document only can `t` value is unset or NULL_OBJECTID
        let query = {_id: self._id, $or: [{t: DEFINE.NULL_OBJECTID},
                                          {t: {$exists: false}}]};
        utils.addShardKeyDatas(pseudoModel, self, query);
        // TODO: need delta cross check
        let checkFields = {t: 1};
        let data = await atomic.findAndModify(pseudoModel.connection,
                                              self.collection, query, delta,
                                              checkFields);

        let hint = {collection: ModelMap.getCollectionName(self),
                    doc: self._id, query: query};
        if (!data) {
            throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_1,
                                       hint);
        }
        if (data.t && !DEFINE.NULL_OBJECTID.equals(data.t)) {
            throw new TransactionError(ERROR_TYPE.TRANSACTION_CONFLICT_2,
                                       hint);
        }
        self.emit('transactedSave');
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
};

module.exports.plugin = (schema) => {
    schema.add({t: {type: mongoose.Schema.Types.ObjectId,
                    'default': DEFINE.NULL_OBJECTID}});
    schema.add({__new: Boolean});

    if (DEFINE.MONGOOSE_VERSIONS[0] < 4) {
        schema.post('init', function() {
            let self = this;
            self._oldSave = self.save;
            self.save = (callback) => {
                self._saveWithoutTransaction((err) => {
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

const filterTransactedDocs = async (docs, callback) => {
    let promise = (async() => {
        if (!docs) {
            throw new Error('invalid documents');
        }
        let transactionIdDocsMap = {};
        let transactionIds = [];
        let result = {ids: transactionIds,
                      map: transactionIdDocsMap};
        if (docs.nextObject) {
            let doc = true;
            while (doc) {
                doc = await helper.nextObject(docs);
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
    })();

    if (callback) {
        return promise.then((ret) => callback(null, ret)).catch(callback);
    }
    return promise;
};

const recheckTransactions = async (model, transactedDocs, callback) => {
    let transactionIds = transactedDocs.ids;
    let transactionIdDocsMap = transactedDocs.map;

    let promise = (async() => {
        for(let i = 0; i < transactionIds.length; i += 1) {
            let transactionId = transactionIds[i];
            let query = {
                _id: transactionId,
            };
            TransactionSchema.attachShardKey(query);
            let transactionModel = ModelMap.getPseudoModel(
                TransactionSchema.RAW_TRANSACTION_COLLECTION
            );
            let Model = transactionModel
                    .connection
                    .models[TransactionSchema.TRANSACTION_COLLECTION];
            let tr = await helper.findOne(Model, query);
            if (tr && tr.state != 'done') {
                await tr._postProcess();
                continue;
            }

            await Promise.each(transactionIdDocsMap[transactionId],
                               async function(doc) {
                let pseudoModel = ModelMap.getPseudoModel(model);
                let query = {_id: doc._id, t: doc.t};
                utils.addShardKeyDatas(pseudoModel, doc, query);
                if (doc.__new) {
                    await helper.remove(model.collection, query);
                    return;
                }
                let updateQuery = {$set: {t: DEFINE.NULL_OBJECTID}};
                await atomic.releaseLock(pseudoModel.connection.db,
                                         model.collection.name,
                                         query, updateQuery);
            });
        }
    })();

    if (callback) {
        return promise.then(callback).catch(callback);
    }
    return promise;
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
const find = (proto, ignoreCallback) => {
    let orig = helper.promisify(proto.target, proto.orig);
    return async function() {
        let args = Array.prototype.slice.call(arguments);
        let callback;
        if (!ignoreCallback) {
            callback = args[args.length - 1];
            if (!callback || typeof(callback) != 'function') {
                // using special case;
                //   `Model.find({}).sort({}).exec(function() {})`
                // FIXME: support this case
                throw new Error('TRANSACTION_FIND_NOT_SUPPORT_QUERY_CHAIN');
            }
            args.pop();
        }
        let promise = (async() => {
            if (args.length > 1) {
                let pseudoModel = ModelMap.getPseudoModel(proto.model);
                let _defaultFields =
                        ['t'].concat(getShardKeyArray(pseudoModel.shardKey));
                args[1] = utils.setDefaultFields(args[1], _defaultFields);
            }

            let RETRY_LIMIT = 10;
            let retry = RETRY_LIMIT;

            for (let i = 0; i < RETRY_LIMIT; i += 1) {
                let docs = await orig(...args);
                if (!proto.isMultiple) {
                    docs = [docs];
                }
                let transactedDocs = await filterTransactedDocs(docs);
                if (!transactedDocs.ids.length) {
                    // FIXME need return
                    docs = proto.isMultiple ? docs : docs[0];
                    if (docs && docs.rewind) {
                        docs.rewind();
                    }
                    return docs;
                }
                await recheckTransactions(proto.model, transactedDocs);
            }
            let hint = {collection: ModelMap.getCollectionName(proto.model),
                        query: args.length > 1 && args[0] || {}};
            throw new TransactionError(ERROR_TYPE.INFINITE_LOOP, hint);
        })();

        if (callback) {
            return promise.then((ret) => callback(null, ret)).catch(callback);
        }
        return promise;
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
const findForce = function(proto) {
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
const findWaitUnlock = function(proto) {
    let _find = find(proto, true);
    return async function() {
        let args = Array.prototype.slice.call(arguments);
        let callback = args[args.length - 1];

        if (!callback || typeof(callback) != 'function') {
            // using special case;
            //   `Model.find({}).sort({}).exec(function() {})`
            // FIXME: support this case
            throw new Error('TRANSACTION_FIND_NOT_SUPPORT_QUERY_CHAIN');
        }
        args.pop();

        let promise = (async() => {
            if (args.length > 1) {
                let pseudoModel = ModelMap.getPseudoModel(proto.model);
                let _defaultFields =
                        ['t'].concat(getShardKeyArray(pseudoModel.shardKey));
                args[1] = utils.setDefaultFields(args[1], _defaultFields);
            }

            let RETRY_LIMIT = 5;
            let lastError;
            for (let i = 0; i < RETRY_LIMIT; i += 1) {
                let docs;
                try {
                    let docs = await _find(...args);
                    return docs;
                } catch(e) {
                    lastError = e;
                }
                await utils.sleep(_.sample([37, 59, 139]));
            }
            throw lastError;
        })();

        if (callback) {
            return promise.then((ret) => callback(null, ret)).catch(callback);
        }
        return promise;
    };
};

module.exports.TransactedModel = (connection, modelName, schema) => {
    schema.plugin(module.exports.plugin);
    let model = connection.model(modelName, schema);
    ModelMap.addCollectionPseudoModelPair(model.collection.name, connection,
                                          schema);

    let toJSON = model.prototype.toJSON;
    model.prototype.toJSON = function transactedModelToJSON(options) {
        let res = toJSON.call(this, options);
        if (res.t && DEFINE.NULL_OBJECTID.equals(res.t)) {
            delete res.t;
        }
        return res;
    };

    let prototypes = [
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
        let methodName = proto.alternative || proto.name;
        proto.model = model;
        proto.orig = proto.target[proto.name];
        // FIXME
        proto.target[proto.name + 'Force'] = proto.orig;
        model[methodName] = findWaitUnlock(proto);
        model[methodName + 'Force'] = findForce(proto);
    });

    // syntactic sugar
    ['', 'Force'].forEach(function (lock) {
        model['findById' + lock] = function (...args) {
            args[0] = {_id: args[0]};
            return this['findOne' + lock].apply(this, args);
        };
    });

    return model;
};
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
