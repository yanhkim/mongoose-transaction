'use strict';

// old version mongo/mongoose functions will stuck with async/await process
const promisify = (method) => {
    return ((obj, ...args) => {
        obj[method].apply(obj, args);
    }).promise;
}

module.exports = {
    findOne: promisify('findOne'),
    update: promisify('update'),
    findAndModify: promisify('findAndModify'),
    executeDbCommand: promisify('executeDbCommand'),
    nextObject: promisify('nextObject'),
    validate: promisify('validate'),
}
