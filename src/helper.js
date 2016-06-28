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
    // old version mongo/mongoose functions will stuck with async/await process
    insert: wrap('insert'),
    find: wrap('find'),
    findOne: wrap('findOne'),
    findAndModify: wrap('findAndModify'),
    findOneAndUpdate: wrap('findOneAndUpdate'),
    update: wrap('update'),
    save: wrap('save'),
    remove: wrap('remove'),
    executeDbCommand: wrap('executeDbCommand'),
    nextObject: wrap('nextObject'),
    validate: wrap('validate'),
    toArray: wrap('toArray'),
    promisify: promisify,
}
