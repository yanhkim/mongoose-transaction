'use strict';
var util = require('util');

var TransactionError = module.exports.TransactionError =
function TransactionError(type, hint) {
    Error.call(this);
    Error.captureStackTrace(this, TransactionError);
    this.name = 'TransactionError';
    this.message = this.type = type || 'unknown';
    this.hint = hint;
};
util.inherits(TransactionError, Error);

module.exports = TransactionError;
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
