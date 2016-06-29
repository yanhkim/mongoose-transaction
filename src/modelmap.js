'use strict';
const TransactionError = require('./error');
const DEFINE = require('./define');
const ERROR_TYPE = DEFINE.ERROR_TYPE;

const CollectionPseudoModelMap = {};

const getCollectionName = (model) => {
    if (model.collection && model.collection.name) {
        return model.collection.name;
    } else {
        return model;
    }
};

const getPseudoModel = (model) => {
    if (!model) {
        throw new TransactionError(ERROR_TYPE.INVALID_COLLECTION);
    }
    let key = getCollectionName(model);
    let pseudoModel = CollectionPseudoModelMap[key];
    if (!pseudoModel) {
        throw new TransactionError(ERROR_TYPE.INVALID_COLLECTION,
                                   {collection: key});
    }
    return pseudoModel;
};

const addCollectionPseudoModelPair = (collectionName, connection, schema) => {
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
