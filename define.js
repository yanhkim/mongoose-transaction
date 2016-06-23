var mongoose = require('mongoose');

var MONGOOSE_VERSIONS = mongoose.version.split('.').map(function(x) {
    return parseInt(x, 10 );
});

var ERROR_TYPE = {
    BROKEN_DATA: 40,
    SOMETHING_WRONG: 41, // data not found or mongo response error
    TRANSACTION_CONFLICT_1: 42, // sequence save
    TRANSACTION_CONFLICT_2: 43, // transacted lock
    TRANSACTION_EXPIRED: 44,
    COMMON_ERROR_RETRY: 45,
    JUST_RETRY: 46,
    INVALID_COLLECTION: 50,
    UNKNOWN_COMMIT_ERROR: 60,
    INFINITE_LOOP: 70,
};

module.exports = {
    MONGOOSE_VERSIONS: MONGOOSE_VERSIONS,
    ERROR_TYPE: ERROR_TYPE,
    NULL_OBJECTID: mongoose.Types.ObjectId("000000000000000000000000"),
}
