'use strict';
require('songbird');
const mongoose = require('mongoose');

const wrapMongoOp = (op) => {
    let key, val;
    for (key in op) {
        if (op.hasOwnProperty(key)) {
            val = op[key];
            if (val === null || val === undefined) {
                continue;
            } else if (val instanceof mongoose.Types.ObjectId) {
                op[key] = {$oid: val.toString()};
            } else if (op[key] instanceof Date) {
                op[key] = {$date: +val};
            } else if (Array.isArray(val) || typeof val === 'object') {
                wrapMongoOp(val);
            }
        }
    }
    return op;
};

const unwrapMongoOp = (op) => {
    let key, val;
    for (key in op) {
        if (op.hasOwnProperty(key)) {
            val = op[key];
            if (Array.isArray(val)) {
                unwrapMongoOp(val);
            } else if (val === null || val === undefined) {
                continue;
            } else if (typeof val === 'object') {
                if (val.hasOwnProperty('$oid')) {
                    op[key] = new mongoose.Types.ObjectId(val.$oid);
                } else if (val.hasOwnProperty('$date')) {
                    op[key] = new Date(val.$date);
                } else {
                    unwrapMongoOp(val);
                }
            }
        }
    }
    return op;
};

const checkExcludeOnly = (fields) => {
    const ret = fields.some((field) => {
        if (field.indexOf('-') === 0) {
            return;
        }
        return true;
    });
    return !ret;
};

const filterExcludedField = (fields, blacklist) => {
    let excludedFields = JSON.parse(JSON.stringify(fields));

    const _excludedFields = [];
    blacklist.forEach((field) => {
        let parentField;
        let topField;
        if (field.indexOf('.') < 0) {
            parentField = field;
            topField = field;
        } else {
            parentField = field.split('.').slice(0, -1).join('.');
            topField = field.split('.')[0];
        }

        const idx = [];
        for (let i = 0; i < excludedFields.length; i += 1) {
            if (excludedFields[i].indexOf('.') < 0) {
                if (excludedFields[i] === topField) {
                    idx.push(i);
                }
            } else {
                if (excludedFields[i].indexOf(parentField + '.') === 0) {
                    idx.push(i);
                }
            }
        }

        if (idx.length > 0) {
            for (let j = 0; j < excludedFields.length; j += 1) {
                if (idx.indexOf(j) < 0) {
                    _excludedFields.push(excludedFields[j]);
                }
            }
            excludedFields = _excludedFields;
        }
    });

    return excludedFields;
};

const mergeStringFields = (srcFields, defaultFields) => {
    let excludedFields = [];
    srcFields.forEach((field) => {
        excludedFields.push(field.slice(1));
    });
    excludedFields = filterExcludedField(excludedFields, defaultFields);
    excludedFields = excludedFields.map((field) => {
        return '-' + field;
    });
    return excludedFields.join(' ');
};

const cleanUpObjectFields = (fields) => {
    let excludedFields = [];
    const includedFields = [];
    Object.keys(fields).forEach((field) => {
        if (fields[field]) {
            includedFields.push(field);
        } else {
            excludedFields.push(field);
        }
    });

    if (excludedFields.length === 0 || includedFields.length === 0) {
        return fields;
    }

    excludedFields = filterExcludedField(excludedFields, includedFields);

    const ret = {};
    excludedFields.forEach((field) => (ret[field] = 0));

    return ret;
};

const setDefaultFields = (srcFields, defaultFields) => {
    if (!srcFields) {
        return srcFields;
    }

    switch (typeof srcFields) {
        case 'string':
            const fieldArray = srcFields.split(' ');
            if (checkExcludeOnly(fieldArray)) {
                return mergeStringFields(fieldArray, defaultFields);
            }
            defaultFields.forEach((field) => {
                if (!field) {
                    return;
                }
                if (fieldArray.indexOf(field) < 0) {
                    fieldArray.push(field);
                }
            });
            return fieldArray.join(' ');
        case 'object':
            if (!Object.keys(srcFields).length) {
                return srcFields;
            }
            defaultFields.forEach((field) => {
                if (!field) {
                    return;
                }
                if (!srcFields[field]) {
                    srcFields[field] = 1;
                }
            });
            return cleanUpObjectFields(srcFields);
        default:
            return srcFields;
    }
};

const extractDelta = (doc) => {
    return (doc.$__delta() || [null, {}])[1];
};

const NODE_VERSIONS =
        process.version.replace('v', '').split('.').map(Math.floor);

let nextTick;
if (NODE_VERSIONS[0] >= 0 && NODE_VERSIONS[1] >= 10) {
    if (global.setImmediate) {
        nextTick = global.setImmediate;
    } else {
        const timers = require('timers');
        if (timers.setImmediate) {
            nextTick = () => {
                timers.setImmediate.apply(this, arguments);
            };
        }
    }
}
nextTick = nextTick || process.nextTick;

const DEBUG = () => {
    if (global.TRANSACTION_DEBUG_LOG) {
        // eslint-disable-next-line
        console.log.apply(console, arguments);
    }
};

const addShardKeyDatas = (pseudoModel, src, dest) => {
    if (!pseudoModel || !pseudoModel.shardKey ||
            !Array.isArray(pseudoModel.shardKey)) {
        return;
    }
    pseudoModel.shardKey.forEach((sk) => { dest[sk] = src[sk]; });
};

const addShardKeyFields = (pseudoModel, dest) => {
    if (!pseudoModel || !pseudoModel.shardKey ||
            !Array.isArray(pseudoModel.shardKey)) {
        return;
    }
    if (!dest || !Object.keys(dest).length) {
        return;
    }
    pseudoModel.shardKey.forEach((sk) => { dest[sk] = 1; });
};

const addUniqueKeyDatas = (pseudoModel, src, dest) => {
    if (!pseudoModel || !pseudoModel.uniqueKey ||
            !Array.isArray(pseudoModel.uniqueKey)) {
        return;
    }
    pseudoModel.uniqueKey.forEach((k) => { dest[k] = src[k]; });
};

const removeShardKeySetData = (shardKey, op) => {
    if (!shardKey || !Array.isArray(shardKey)) {
        return;
    }
    if (!op.$set) {
        return;
    }
    shardKey.forEach((sk) => {
        delete op.$set[sk];
    });
};

const sleep = ((microsec, callback) => {
    if (microsec <= 0) {
        return nextTick(callback);
    }
    setTimeout(callback, microsec);
}).promise;

const promisify = (obj, method) => {
    if (typeof method === 'string') {
        method = obj[method];
    }
    return ((...args) => {
        method.apply(obj, args);
    }).promise;
};

module.exports = {
    DEBUG: DEBUG,
    wrapMongoOp: wrapMongoOp,
    unwrapMongoOp: unwrapMongoOp,
    setDefaultFields: setDefaultFields,
    extractDelta: extractDelta,
    nextTick: nextTick || process.nextTick,
    addShardKeyDatas: addShardKeyDatas,
    addShardKeyFields: addShardKeyFields,
    removeShardKeySetData: removeShardKeySetData,
    addUniqueKeyDatas: addUniqueKeyDatas,
    sleep: sleep,
    promisify: promisify,
};
// vim: et ts=5 sw=4 sts=4 colorcolumn=80
