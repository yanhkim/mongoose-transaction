"use strict";
var should = require('should');
var mongoose = require('mongoose');
var async = require('async');
var sync = require('synchronize');
global.TRANSACTION_DEBUG_LOG = false;
var transaction = require('../index');
var ERRORS = transaction.TRANSACTION_ERRORS;

var connection;
var Test;
var Transaction;


var initialize = function (callback) {
    var config;
    try {
        config = require('./config');
    } catch(e) {
        config = {mongodb: 'localhost:27017'};
    }
    var dbname = 'test_transaction_' + (+new Date());
    var uri = 'mongodb://' + config.mongodb + '/' + dbname;
    connection = mongoose.createConnection(uri, callback);
};

var TestSchema = new mongoose.Schema({
    num: {type: Number, max: 5},
    string: String,
    def: {type: Number, required: true, default: 1}
}, {shardKey: {_id: 1}});

var getNative = function(callback) {
    this.collection.findOne({_id: this._id}, callback);
};

TestSchema.methods.getNative = getNative;
transaction.TransactionSchema.methods.getNative = getNative;

before(function (done) {
    initialize(function (err) {
        if (err) {
            return done(err);
        }
        Test = transaction.TransactedModel(connection, 'Test', TestSchema);
        // FIXME: need init process
        transaction.TransactionSchema.plugin(function(schema) {
            schema.add({shard: {type: Number, required: true}});
            schema.options.shardKey = {shard: 1, _id: 1};
            schema.pre('validate', function(cb) {
                if (!this.shard) {
                    this.shard = this._id.getTimestamp().getTime();
                }
                cb();
            });
        });
        Transaction = connection.model(transaction.TRANSACTION_COLLECTION,
                                       transaction.TransactionSchema);
        transaction.addCollectionPseudoModelPair(
            Transaction.collection.name, connection,
            transaction.TransactionSchema
        );
        done();
    });
});

var wrapTransactionMethods = function(tr) {
    if (!tr) {
        return;
    }
    var methods = ['begin', 'add', 'removeDoc', 'commit', 'cancel', 'expire',
                   'remove', 'getNative'];
    methods.forEach(function(method) {
        sync(tr, method);
    });
};

var wrapDocumentMethods = function(doc) {
    if (!doc) {
        return;
    }
    var methods = ['save', 'remove', 'getNative'];
    methods.forEach(function(method) {
        sync(doc, method);
    });
};

var newTest = function(opts) {
    var test = new Test(opts);
    wrapDocumentMethods(test);
    return test;
};

var beginTransaction = function(callback) {
    Transaction.begin(function(err, tr) {
        if (err) {
            return callback(err);
        }
        wrapTransactionMethods(tr);
        callback(null, tr);
    });
};

beforeEach(function(done) {
    var self = this;
	self.timeout(10000);
    async.parallel({
        x: function(next) {
            var x = newTest({num: 1});
            x.save(function(err) {
                next(err, x);
            });
        },
        t: beginTransaction,
    }, function(err, result) {
        if (err) {
            return done(err);
        }
        result = result || {};
        self.t = result.t;
        wrapTransactionMethods(self.t);
        self.x = result.x;
        done();
    });
});

afterEach(function(done) {
    if (!connection || !connection.db) {
        return done();
    }
    connection.db.dropDatabase(done);
});


describe('TransactedModel', function() {
    it('should have transaction lock at create new doucment', function() {
        this.x.t.should.eql(transaction.NULL_OBJECTID);
    });

    it('should have transaction lock at fetch document from database',
       function(done) {
        var self = this;
        sync.fiber(function() {
            Test.findById(self.x._id, sync.defer());
            sync.await().t.should.eql(transaction.NULL_OBJECTID);
        }, done);
    });
    it('should fetch lock and sharding fields if not exists at fetch targets',
       function(done) {
        var self = this;
        sync.fiber(function() {
            Test.findById(self.x._id, 'num', sync.defer());
            var test = sync.await();
            should.exists(test.t);
            should.exists(test._id);
        }, done);
    });

    it('result of toJSON should remove lock field',
       function(done) {
        var self = this;
        sync.fiber(function() {
            Test.findById(self.x._id, sync.defer());
            sync.await().toJSON().should.not.have.property('t');
        }, done);
    });

    it('can be try fetch non exist document', function(done) {
        var id = new mongoose.Types.ObjectId();
        Test.findOne({_id: id}, function (err, doc) {
            should.not.exists(doc);
            done(err);
        });
    });
});

describe('Save with transaction', function() {
    it('transaction add should check validate schema',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.x.num = 10;
            try {
                self.t.add(self.x);
                should.fail('no error was thrown');
            } catch(e) {
                e.name.should.eql('ValidationError');
            }
        }, done);
    });

    it('update can be possible', function(done) {
        var self = this;
        sync.fiber(function() {
            self.x.num = 2;
            self.t.add(self.x);
            var _x1 = self.x.getNative();
            self.t._id.should.eql(_x1.t);
            _x1.num.should.eql(1);
            _x1.def.should.eql(1);
            self.t.commit();
            var _x2 = self.x.getNative();
            _x2.t.should.eql(transaction.NULL_OBJECTID);
            _x2.num.should.eql(2);
            should.not.exists(self.t.getNative());
        }, done);
    });

    it('can make new document', function(done) {
        var self = this;
        sync.fiber(function() {
            var x = newTest({num: 1});
            self.t.add(x);
            var _x1 = x.getNative();
            self.t._id.should.eql(_x1.t);
            should.not.exist(_x1.num);
            _x1.__new.should.eql(true);
            self.t.commit();
            var _x2 = x.getNative();
            _x2.t.should.eql(transaction.NULL_OBJECTID);
            _x2.num.should.eql(1);
            _x2.def.should.eql(1);
            should.not.exists(_x2.__new);
            should.not.exists(self.t.getNative());
        }, done);
    });

    it('if cancel transaction process and contains new documents, ' +
           'should cancel make new documents',
       function(done) {
        var self = this;
        sync.fiber(function() {
            var x = newTest({num: 1});
            self.t.add(x);
            var _x = x.getNative();
            should.exists(_x);
            self.t._id.should.eql(_x.t);
            should.not.exist(_x.num);
            try {
                self.t.cancel('testcase');
            } catch(e) {}
            should.not.exists(x.getNative());
            should.not.exists(self.t.getNative());
        }, done);
    });

    it('if stop in the middle of transaction process,' +
            'should cancel make new documents', function(done) {
        var self = this;
        sync.fiber(function() {
            var x = newTest({num: 1});
            self.t.add(x);
            self.t.remove();
            Test.findWithWaitRetry({_id: x._id}, sync.defer());
            var _x = sync.await();
            should.exists(_x);
            _x.length.should.eql(0);
        }, done);
    });

    it('should support multiple documents with transaction',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.x.num = 2;
            self.t.add(self.x);
            var y = newTest({string: 'abcd'});
            self.t.add(y);
            self.t.commit();
            self.x.getNative().num.should.eql(2);
            y.getNative().string.should.eql('abcd');
            should.not.exists(self.t.getNative());
        }, done);
    });

    it('should support remove document with transaction',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t.removeDoc(self.x);
            self.t.commit();
            should.not.exists(self.x.getNative());
        }, done);
    });

    it('if cancel transaction process, also cancel reserved remove document',
       function(done) {
        var self  = this;
        sync.fiber(function() {
            self.t.removeDoc(self.x);
            self.t.expire();
            should.exists(self.x.getNative());
        }, done);
    });

});

describe('Find documents from model', function() {
    it('auto commit before load data', function (done) {
        var self = this;
        sync.fiber(function() {
            self.x.num = 2;
            self.t.add(self.x);
            self.t._commit(sync.defer()); sync.await();
            Test.findById(self.x.id, sync.defer());
            var _x = sync.await();
            _x.t.should.eql(transaction.NULL_OBJECTID);
            _x.num.should.eql(2);
        }, done);
    });

    it('find fetch all documents of matched, ' +
            'they should finish commit process of previous transaction',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.x.num = 2;
            self.t.add(self.x);
            var y = newTest({num: 1});
            y.save();
            y.num = 2;
            self.t.add(y);
            self.t._commit(sync.defer()); sync.await();
            Test.find({}, sync.defer());
            var docs = sync.await();
            should.exists(docs);
            docs.length.should.eql(2);
            docs.forEach(function(d) {
                d.t.should.eql(transaction.NULL_OBJECTID);
                d.num.should.eql(2);
            });
        }, done);
    });

    it('findById fetch a document, ' +
            'it should cancel removed previous transaction',
       function(done) {
        var self = this;
        sync.fiber(function() {
            Test.collection.update({_id: self.x._id},
                                   {$set: {t: new mongoose.Types.ObjectId()}},
                                   sync.defer()); sync.await();
            Test.findById(self.x._id, sync.defer());
            var _x = sync.await();
            should.exists(_x);
            _x.t.should.eql(transaction.NULL_OBJECTID);
        }, done);
    });

    it('find fetch all documents of matched, ' +
            'they should cancel removed previous transaction',
       function(done) {
        var self = this;
        sync.fiber(function() {
            Test.collection.update({_id: self.x._id},
                                   {$set: {t: new mongoose.Types.ObjectId()}},
                                   sync.defer()); sync.await();
            var x2 = newTest({num: 2, t: new mongoose.Types.ObjectId()});
            x2.save(sync.defer()); sync.await();
            Test.find({}, sync.defer());
            var xs = sync.await();
            should.exists(xs);
            xs.length.should.eql(2);
            xs.forEach(function (x) {
                x.t.should.eql(transaction.NULL_OBJECTID);
            });
        }, done);
    });

    it('findOneWithWaitRetry should wait previous transaction lock',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t.add(self.x);
            //var st = +new Date();
            try {
                Test.findOneWithWaitRetry({_id: self.x._id}, sync.defer());
                sync.await();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
            //((+new Date()) - st >= 37 * 5).should.be.true;
        }, done);
    });

    it('findOneNatvie fetch a native mongo document of matched, ' +
            'it should cancel removed previous transaction',
       function(done) {
        sync.fiber(function() {
            var x = newTest({num: 1, t: new mongoose.Types.ObjectId()});
            x.save();
            Test.findOneNative({_id: x._id}, sync.defer());
            var _x = sync.await();
            should.exists(_x);
            _x.t.should.eql(transaction.NULL_OBJECTID);
        }, done);
    });

    it('findNatvie fetch all native mongo documents of matched, ' +
            'they should cancel removed previous transaction',
       function(done) {
        sync.fiber(function() {
            var x1 = newTest({num: 1, t: new mongoose.Types.ObjectId()});
            var x2 = newTest({num: 1, t: new mongoose.Types.ObjectId()});
            async.forEach([x1, x2], function(doc, next) {
                doc.save(next);
            }, sync.defer()); sync.await();
            Test.findNative({}, sync.defer());
            var xs = sync.await();
            should.exists(xs);
            xs.count(sync.defer());
            var count = sync.await();
            should.exists(count);
            count.should.not.eql(0);
            xs.rewind();
            xs.toArray(sync.defer());
            var _xs = sync.await();
            should.exists(_xs);
            _xs.length.should.eql(count);
            _xs.forEach(function(x) {
                x.t.should.eql(transaction.NULL_OBJECTID);
            });
        }, done);
    });

    it('can be force fetch document, ignoring transaction lock',
       function(done) {
       var self = this;
       sync.fiber(function() {
            self.t.add(self.x);
            Test.findOneForce({_id: self.x._id}, sync.defer());
            var _x = sync.await();
            should.exists(_x);
            _x.t.should.eql(self.t._id);
        }, done);
    });

});

describe('Find documents from transaction', function() {
    it('findOne fetch a document and automatic set transaction lock',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t.findOne(Test, {_id: self.x._id}, sync.defer());
            var x1 = sync.await();
            should.exist(x1);
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(x1.t);
            x1.num = 2;
            self.t.commit();
            self.x.getNative().num.should.eql(2);
        }, done);
    });

    // FIXME: findOne muse try to remove `t`
    xit('findOne fetch a document of matched, ' +
            'it should finish commit process of previous transaction',
        function(done) {
        var self = this;
        sync.fiber(function() {
            self.x.num = 2;
            self.t.add(self.x);
            self.t._commit(sync.defer()); sync.await();
            beginTransaction(sync.defer());
            var t2 = sync.await();
            // FIXME: make TransactionSchema.findById
            //t2.findById(Test, x.id, sync.defer());
            t2.findOne(Test, {_id: self.x._id}, sync.defer());
            var _x = sync.await();
            should.exists(_x);
            _x.t.should.eql(transaction.NULL_OBJECTID);
            _x.num.should.eql(2);
        }, done);
    });

    it('find fetch documents and automatic set transaction lock',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t.find(Test, {_id: self.x._id}, sync.defer());
            var docs = sync.await();
            should.exist(docs);
            Array.isArray(docs).should.be.true;
            docs.length.should.be.eql(1);
            var x1 = docs[0];
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(x1.t);
            x1.num = 2;
            self.t.commit();
            self.x.getNative().num.should.eql(2);
        }, done);
    });

    it('Transaction.findOne should support sort option',
       function (done) {
        var self = this;
        sync.fiber(function() {
            (newTest()).save();
            (newTest()).save();
            self.t.findOne(Test, null, {sort: {'_id': 1}}, sync.defer());
            var t1 = sync.await();
            should.exist(t1);
            self.t.findOne(Test, null, {sort: {'_id': -1}}, sync.defer());
            var t2 = sync.await();
            should.exist(t2);
            t1._id.should.not.eql(t2._id);
        }, done);
    });

    // FIXME: current find only check t is `NULL_OBJECTID`
    // so, docs.length is always return 0
    xit('find fetch all documents of matched, ' +
            'they should finish commit process of previous transaction',
        function(done) {
        var self = this;
        sync.fiber(function() {
            self.x.num = 2;
            self.t.add(self.x);
            var y = newTest({num: 1});
            y.save();
            y.num = 2;
            self.t.add(y);
            self.t._commit(sync.defer()); sync.await();
            beginTransaction(sync.defer());
            var t2 = sync.await();
            t2.find(Test, {}, sync.defer());
            var docs = sync.await();
            should.exists(docs);
            docs.length.should.eql(2);
            docs.forEach(function(d) {
                d.t.should.eql(transaction.NULL_OBJECTID);
                d.num.should.eql(2);
            });
        }, done);
    });

    // FIXME: current findOne not try post process, fail document fetch
    // also, we need test of document exist
    xit('findOne fetch a document of matched, ' +
            'it should finish commit process of previous transaction',
        function(done) {
        var self = this;
        sync.fiber(function() {
            var x = newTest({num: 1, t: new mongoose.Types.ObjectId()});
            x.save();
            //t.findById(Test, x._id, sync.defer());
            self.t.findOne(Test, {_id: x._id}, sync.defer());
            var _x = sync.await();
            should.exists(_x);
            _x.t.should.eql(self.t._id);
        }, done);
    });

    // FIXME: current find only check t is `NULL_OBJECTID`
    // so, docs.length is always return 0
    xit('find fetch all documents of matched, ' +
            'they should cancel removed previous transaction',
        function(done) {
        var self = this;
        sync.fiber(function() {
            var x1 = newTest({num: 1, t: new mongoose.Types.ObjectId()});
            var x2 = newTest({num: 2, t: new mongoose.Types.ObjectId()});
            async.forEach([x1, x2], function (doc, next) {
                doc.save(next);
            }, sync.defer()); sync.await();
            self.t.find(Test, {}, sync.defer());
            var xs = sync.await();
            should.exists(xs);
            xs.length.should.eql(2);
            xs.forEach(function (x) {
                x.t.should.eql(transaction.NULL_OBJECTID);
            });
        }, done);
    });

    it('findWithWaitRetry fetch documents and automatic set transaction lock',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t.findWithWaitRetry(Test, {_id: self.x._id}, sync.defer());
            var docs = sync.await();
            should.exist(docs);
            Array.isArray(docs).should.be.true;
            docs.length.should.be.eql(1);
            var x1 = docs[0];
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(x1.t);
            x1.num = 2;
            self.t.commit();
            self.x.getNative().num.should.eql(2);
        }, done);
    });

});

describe('Transaction conflict', function() {
    it('above two transaction mark manage document mark at the same time',
            function(done) {
        var self = this;
        sync.fiber(function() {
            beginTransaction(sync.defer());
            var t1 = sync.await();
            Test.findById(self.x.id, sync.defer());
            var x1 = sync.await();
            self.x.num = 2;
            self.t.add(self.x);
            x1.num = 3;
            try {
                t1.add(x1);
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_1);
            }
        }, done);
    });

    it('already transacted document try save on another process',
            function(done) {
        var self = this;
        sync.fiber(function() {
            Test.findById(self.x.id, sync.defer());
            var x1 = sync.await();
            self.x.num = 2;
            self.t.add(self.x);
            x1.num = 3;
            try {
                x1.save(sync.defer()); sync.await();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_1);
            }
            self.t.commit();
            var _x = self.x.getNative();
            _x.t.should.eql(transaction.NULL_OBJECTID);
            _x.num.should.eql(2);
        }, done);
    });

    it('(normal)not transacted document try save on another process',
            function(done) {
        var self = this;
        sync.fiber(function() {
            Test.findOne({_id: self.x._id}, sync.defer());
            var _x1 = sync.await();
            _x1.num = 2;
            _x1.save(sync.defer()); sync.await();
            var _x2 = self.x.getNative();
            _x2.t.should.eql(transaction.NULL_OBJECTID);
            _x2.num.should.eql(2);
        }, done);
    });

    it.skip('(broken) we cannot care manually sequential update ' +
                'as fetched document without transaction',
            function(done) {
        sync.fiber(function() {
            var x = newTest({num: 1});
            x.save(sync.defer()); sync.await();
            Test.findOne({_id: x._id}, sync.defer());
            var x1 = sync.await();
            Test.findOne({_id: x._id}, sync.defer());
            var x2 = sync.await();
            beginTransaction(sync.defer());
            var t1 = sync.await();
            x1.num = 2;
            t1.add(x1);
            t1.commit();
            beginTransaction(sync.defer());
            var t2 = sync.await();
            x2.num = 3;
            t2.add(x2);
            should.fail('no error was thrown');
            t2.commit();
        }, done);
    });

    it('findOne from transaction prevent race condition when fetch a document',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t.findOne(Test, {_id: self.x._id}, sync.defer());
            var x1 = sync.await();
            should.exist(x1);
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            try {
                self.t.findOne(Test, {_id: self.x._id}, sync.defer());
                sync.await();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        }, done);
    });
});

describe('Transaction lock', function() {
    it('model.findById should raise error at try fetch to locked document',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.x.num = 2;
            self.t.add(self.x);
            try {
                Test.findById(self.x.id, sync.defer()); sync.await();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        }, done);
    });

    // FIXME: wait lock feature should be default option of findOne
    xit('model.findOne should wait unlock previous transaction lock',
        function(done) {
        var self = this;
        sync.fiber(function() {
            self.t.add(self.x);
            async.parallel([
                function (next) {
                    Test.findOne({_id: self.x._id}, function (err, _x) {
                        should.not.exists(err);
                        should.exists(_x);
                        _x.t.should.eql(transaction.NULL_OBJECTID);
                        next();
                    });
                },
                function (next) {
                    setTimeout(function() {
                        self.t.commit(next);
                    }, 100);
                }
            ], sync.defer()); sync.await();
        }, done);
    });

    it('model.findOneWithWaitRetry should wait ' +
            'unlock previous transaction lock',
       function(done) {
        var self = this;
        sync.fiber(function() {
            var x = newTest({num: 1, t: new mongoose.Types.ObjectId()});
            self.t.add(x);
            async.parallel([
                function (next) {
                    Test.findOneWithWaitRetry({_id: self.x._id},
                                              function (err, _x) {
                        should.not.exists(err);
                        should.exists(_x);
                        _x.t.should.eql(transaction.NULL_OBJECTID);
                        next();
                    });
                },
                function (next) {
                    setTimeout(function() {
                        self.t.commit(next);
                    }, 100);
                }
            ], sync.defer()); sync.await();
        }, done);
    });

    // FIXME: current findOne not try post process, fail document fetch
    // also, we need test of document exist
    xit('transaction.findOne should raise error ' +
            'at try fetch to locked document',
        function(done) {
        var self = this;
        sync.fiber(function() {
            self.x.num = 2;
            self.t.add(self.x);
            beginTransaction(sync.defer());
            var t1 = sync.await();
            try {
                t1.findOne(Test, {_id: self.x.id}, sync.defer()); sync.await();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        }, done);
    });

    it('transaction.findOneWithWaitRetry should raise error ' +
            'at try fetch to locked document ' +
            'and previous transaction was alive',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t.add(self.x);
            beginTransaction(sync.defer());
            var t1 = sync.await();
            //var st = +new Date();
            try {
                t1.findOneWithWaitRetry(Test, {_id: self.x._id}, sync.defer());
                sync.await();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
            //((+new Date()) - st >= 37 * 5).should.be.true;
        }, done);
    });

    it('transaction.findOneWithWaitRetry should wait ' +
            'unlock previous transaction lock',
       function(done) {
        var self = this;
        sync.fiber(function() {
            beginTransaction(sync.defer());
            var t1 = sync.await();
            self.t.add(self.x);
            async.parallel([
                function (next) {
                    t1.findOneWithWaitRetry(Test, {_id: self.x._id},
                                            function (err, _x) {
                        should.not.exists(err);
                        should.exists(_x);
                        _x.t.should.eql(t1._id);
                        next();
                    });
                },
                function (next) {
                    setTimeout(function() {
                        self.t.commit(next);
                    }, 100);
                }
            ], sync.defer()); sync.await();
        }, done);
    });

    // FIXME: wait lock feature should be default option of findOne
    xit('transaction.findOne should wait unlock previous transaction lock',
        function(done) {
        var self = this;
        sync.fiber(function() {
            beginTransaction(sync.defer());
            var t1 = sync.await();
            self.t.add(self.x);
            async.parallel([
                function (next) {
                    t1.findOne(Test, {_id: self.x._id}, function (err, _x) {
                        should.not.exists(err);
                        should.exists(_x);
                        _x.t.should.eql(transaction.NULL_OBJECTID);
                        next();
                    });
                },
                function (next) {
                    setTimeout(function() {
                        self.t.commit(next);
                    }, 100);
                }
            ], sync.defer()); sync.await();
        }, done);
    });

    it('overtime transaction should expire automatically',
            function(done) {
        sync.fiber(function() {
            var beforeGap =
                    +new Date() - transaction.TRANSACTION_EXPIRE_GAP;
            var t = new Transaction({
                _id: mongoose.Types.ObjectId.createFromTime(beforeGap)
            });
            wrapTransactionMethods(t);
            var x = newTest({num: 1});
            t.begin();
            t.add(x);
            try {
                t.commit();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.TRANSACTION_EXPIRED);
            }
            should.not.exists(x.getNative());
        }, done);
    });
});

describe('Transaction state conflict', function() {
    it('already committed transaction cannot move expire state',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t._commit(sync.defer()); sync.await();
            self.t.expire();
            self.t.state.should.eql('commit');
        }, done);
    });

    it('already expired transaction cannot move commit state',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t._expire(sync.defer()); sync.await();
            try {
                self.t.commit();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.TRANSACTION_EXPIRED);
            }
            self.t.state.should.eql('expire');
        }, done);
    });

    it('if transaction expired for another process, cannot move commit state',
       function(done) {
        var self = this;
        sync.fiber(function() {
            Transaction.findById(self.t._id, sync.defer());
            var t1 = sync.await();
            t1.state = 'expire';
            t1.save(sync.defer());
            sync.await();
            try {
                self.t.commit();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.UNKNOWN_COMMIT_ERROR);
            }
            self.t.state.should.eql('expire');
        }, done);
    });

    it('if transaction committed for another process, ' +
            'cannot move expire state',
       function(done) {
        var self = this;
        sync.fiber(function() {
            Transaction.findById(self.t._id, sync.defer());
            var t1 = sync.await();
            t1.state = 'commit';
            t1.save(sync.defer()); sync.await();
            try {
                self.t.expire();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.SOMETHING_WRONG);
            }
            self.t.state.should.eql('expire');
        }, done);
    });

    it('if transaction committed for another process' +
            'we use persistent data of mongodb',
       function(done) {
        var self = this;
        sync.fiber(function() {
            self.t.add(self.x);
            self.x.num = 3;
            Transaction.findById(self.t._id, sync.defer());
            var t1 = sync.await();
            t1._docs = [];
            var y = newTest({num: 2});
            t1.add(y, sync.defer()); sync.await();
            t1._commit(sync.defer()); sync.await();
            self.t.commit();
            var _x = self.x.getNative();
            should.exists(_x);
            _x.t.should.not.eql(transaction.NULL_OBJECTID);
            _x.num.should.eql(1);
            var _y = y.getNative();
            should.exists(_y);
            _y.t.should.eql(transaction.NULL_OBJECTID);
            _y.num.should.eql(2);
        }, done);
    });

    it('if mongodb raise error when transaction commit, ' +
            'automatically move to expire state',
       function(done) {
        var self = this;
        sync.fiber(function() {
            var save = self.t._moveState;
            var called = false;
            self.t._moveState = function(_, __, cb) {
                if (!called) {
                    called = true;
                    return cb('something wrong');
                }
                save.apply(self.t, arguments);
            };
            try {
                self.t.commit();
                should.fail('no error was thrown');
            } catch(e) {
                e.message.should.eql(ERRORS.UNKNOWN_COMMIT_ERROR);
            }
            self.t.state.should.eql('expire');
        }, done);
    });
});
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
