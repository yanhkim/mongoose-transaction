'use strict';
require('songbird');

// ### insert
// Version independent helper for `insert` query of the mongodb-native
//
// #### Arguments
// * col - :Collection: SeeAlso `node-mongodb-native/lib/collection.js`
// * data - :Object:
//
// #### Return
// :Promise:
const insert = async(col, data) => {
    if (col.insertOne) {
        return await col.promise.insertOne(data);
    } else {
        return await col.promise.insert(data);
    }
};

// ### update
// Version independent helper for `update` query of the mongodb-native
//
// #### Arguments
// * col - :Collection: SeeAlso `node-mongodb-native/lib/collection.js`
// * query - :Object:
// * data - :Object:
// * opts - :Object:
//
// #### Return
// :Promise:
const update = async(col, query, data, opts) => {
    if (col.updateMany) {
        return await col.promise.updateMany(query, data, opts);
    }
    return await col.promise.update(query, data, opts);
};

// ### remove
// Version independent helper for `remove` query of the mongodb-native
//
// #### Arguments
// * col - :Collection: SeeAlso `node-mongodb-native/lib/collection.js`
// * query - :Object:
//
// #### Return
// :Promise:
const remove = async(col, query) => {
    if (col.removeOne) {
        return await col.promise.removeOne(query);
    } else {
        return await col.promise.remove(query);
    }
};

module.exports = {
    insert: insert,
    update: update,
    remove: remove,
};
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
