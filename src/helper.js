'use strict';

const wrap = (method) => {
    return ((obj, ...args) => {
        obj[method].apply(obj, args);
    }).promise;
}

const promisify = (obj, method) => {
    if (typeof method === 'string') {
        method = obj[method];
    }
    return ((...args) => {
        method.apply(obj, args);
    }).promise;
}

module.exports = {
    // FIXME
    // old version mongo/mongoose functions will stuck with async/await process
    insert: wrap('insert'),
    find: wrap('find'),
    findOne: wrap('findOne'),
    findAndModify: wrap('findAndModify'),
    findOneAndUpdate: wrap('findOneAndUpdate'),
    findById: wrap('findById'),
    findNative: wrap('findNative'),
    findOneNative: wrap('findOneNative'),
    findOneForce: wrap('findOneForce'),
    update: wrap('update'),
    save: wrap('save'),
    remove: wrap('remove'),
    count: wrap('count'),
    executeDbCommand: wrap('executeDbCommand'),
    nextObject: wrap('nextObject'),
    validate: wrap('validate'),
    toArray: wrap('toArray'),
    promisify: promisify,
}
