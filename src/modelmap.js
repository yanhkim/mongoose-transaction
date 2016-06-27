'use strict';
const TransactionError = require('./error');
const DEFINE = require('./define');
const ERROR_TYPE = DEFINE.ERROR_TYPE;

const CollectionPseudoModelMap = {};

const getCollectionName = function(model) {
    if (model.collection && model.collection.name) {
        return model.collection.name;
    } else {
        return model;
    }
};

const getPseudoModel = function(model) {
    if (!model) {
        throw new TransactionError(ERROR_TYPE.INVALID_COLLECTION);
    }
    var key = getCollectionName(model);
    var pseudoModel = CollectionPseudoModelMap[key];
    if (!pseudoModel) {
        throw new TransactionError(ERROR_TYPE.INVALID_COLLECTION,
                                   {collection: key});
    }
    return pseudoModel;
};

const addCollectionPseudoModelPair = function(collectionName, connection,
                                              schema) {
    let shardKey;
    if (schema.options && schema.options.shardKey) {
        shardKey = Object.keys(schema.options.shardKey);
    }
    CollectionPseudoModelMap[collectionName] = {
        connection: connection,
        shardKey: shardKey,
    };
};

module.exports = {
    getCollectionName: getCollectionName,
    getPseudoModel: getPseudoModel,
    addCollectionPseudoModelPair: addCollectionPseudoModelPair,
};
// vim: et ts=5 sw=4 sts=4 colorcolumn=80
