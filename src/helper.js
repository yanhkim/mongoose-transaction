'use strict';

const wrap = (method) => {
    return ((obj, ...args) => {
        obj[method].apply(obj, args);
    }).promise;
}

const promisify = (obj, method) => {
    if (typeof method == 'string') {
        method = obj[method];
    }
    return ((...args) => {
        method.apply(obj, args);
    }).promise;
}

module.exports = {
    // old version mongo/mongoose functions will stuck with async/await process
    findOne: wrap('findOne'),
    update: wrap('update'),
    findAndModify: wrap('findAndModify'),
    remove: wrap('remove'),
    executeDbCommand: wrap('executeDbCommand'),
    nextObject: wrap('nextObject'),
    validate: wrap('validate'),
    promisify: promisify,
}
