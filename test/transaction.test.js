/* eslint-env node, mocha */
'use strict';
const Promise = require('songbird');
const should = require('should');
const mongoose = require('mongoose');
global.TRANSACTION_DEBUG_LOG = false;
const transaction = require('../src/index');
const DEFINE = require('../src/define');
const utils = require('../src/utils');
const ERRORS = DEFINE.ERROR_TYPE;

let connection;
let Test;
let Transaction;

const initialize = (callback) => {
    let config;
    try {
        config = require('./config');
    } catch (e) {
        config = {mongodb: 'localhost:27017'};
    }
    let dbname = 'test_transaction_' + (+new Date());
    let uri = 'mongodb://' + config.mongodb + '/' + dbname;
    connection = mongoose.createConnection(uri, callback);
};

const TestSchema = new mongoose.Schema({
    num: {type: Number, max: 5},
    string: String,
    def: {type: Number, required: true, default: 1},
}, {shardKey: {_id: 1}});

const getNative = async function() {
    return await this.collection.promise.findOne({_id: this._id});
};

TestSchema.methods.getNative = getNative;
transaction.TransactionSchema.methods.getNative = getNative;

before(async(done) => {
    try {
        await initialize.promise();
    } catch (e) {
        return done(e);
    }

    Test = transaction.TransactedModel(connection, 'Test', TestSchema);
    // FIXME: need init process
    transaction.TransactionSchema.plugin(
        transaction.bindShardKeyRule,
        {
            fields: {shard: {type: Number, required: true}},
            rule: {shard: 1, _id: 1},
            initialize: (doc) => {
                doc.shard =
                    doc.shard ||
                    doc._id.getTimestamp()
                    .getTime();
            },
        }
    );
    Transaction = connection.model(transaction.TRANSACTION_COLLECTION,
                                   transaction.TransactionSchema);
    transaction.addCollectionPseudoModelPair(
        Transaction.collection.name, connection,
        transaction.TransactionSchema
    );
    done();
});

beforeEach(function(done) {
    var self = this;
    self.timeout(10000);

    let promise = (async() => {
        self.x = new Test({num: 1});
        await self.x.promise.save();
        self.t = await Transaction.begin();
    })();
    promise.then(done).catch(done);
});

afterEach((done) => {
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
        async function() {
            let doc = await Test.promise.findById(this.x._id);
            doc.t.should.eql(transaction.NULL_OBJECTID);
        });

    it('should fetch lock and sharding fields if not exists at fetch targets',
        async function() {
            let test = await Test.promise.findById(this.x._id, 'num');
            should.exists(test.t);
            should.exists(test._id);
        });

    it('result of toJSON should remove lock field', async function() {
        let doc = await Test.promise.findById(this.x._id);
        doc.toJSON().should.not.have.property('t');
    });

    it('can be try fetch non exist document', async function() {
        let id = new mongoose.Types.ObjectId();
        let doc = await Test.promise.findOne({_id: id});
        should.not.exists(doc);
    });
});

describe('Save with transaction', function() {
    it('transaction add should check validate schema', async function() {
        this.x.num = 10;
        try {
            await this.t.add(this.x);
            should.fail('no error was thrown');
        } catch (e) {
            e.name.should.eql('ValidationError');
        }
    });

    it('update can be possible', async function() {
        this.x.num = 2;
        await this.t.add(this.x);
        let x1 = await this.x.getNative();
        this.t._id.should.eql(x1.t);
        x1.num.should.eql(1);
        x1.def.should.eql(1);
        await this.t.commit();
        let x2 = await this.x.getNative();
        x2.t.should.eql(transaction.NULL_OBJECTID);
        x2.num.should.eql(2);
        should.not.exists(await this.t.getNative());
    });

    it('can make new document', async function() {
        let x = new Test({num: 1});
        await this.t.add(x);
        let x1 = await x.getNative();
        this.t._id.should.eql(x1.t);
        should.not.exist(x1.num);
        x1.__new.should.eql(true);
        await this.t.commit();
        let x2 = await x.getNative();
        x2.t.should.eql(transaction.NULL_OBJECTID);
        x2.num.should.eql(1);
        x2.def.should.eql(1);
        should.not.exists(x2.__new);
        should.not.exists(await this.t.getNative());
    });

    it('if cancel transaction process and contains new documents, ' +
        'should cancel make new documents',
        async function() {
            let x = new Test({num: 1});
            await this.t.add(x);
            let x1 = await x.getNative();
            should.exists(x1);
            this.t._id.should.eql(x1.t);
            should.not.exist(x1.num);
            try {
                await this.t.cancel('testcase');
            } catch (e) {}
            should.not.exists(await x.getNative());
            should.not.exists(await this.t.getNative());
        });

    it('if stop in the middle of transaction process,' +
            'should cancel make new documents', async function() {
        let x = new Test({num: 1});
        await this.t.add(x);
        await this.t.remove();
        let xs = await Test.promise.find({_id: x._id});
        should.exists(xs);
        xs.length.should.eql(0);
    });

    it('should support multiple documents with transaction', async function() {
        this.x.num = 2;
        await this.t.add(this.x);
        let y = new Test({string: 'abcd'});
        await this.t.add(y);
        await this.t.commit();
        (await this.x.getNative()).num.should.eql(2);
        (await y.getNative()).string.should.eql('abcd');
        should.not.exists(await this.t.getNative());
    });

    it('should support remove document with transaction', async function() {
        await this.t.removeDoc(this.x);
        await this.t.commit();
        should.not.exists(await this.x.getNative());
    });

    it('if cancel transaction process, also cancel reserved remove document',
        async function() {
            await this.t.removeDoc(this.x);
            await this.t.expire();
            should.exists(await this.x.getNative());
        });
});

describe('Find documents from model', function() {
    it('auto commit before load data', async function() {
        this.x.num = 2;
        await this.t.add(this.x);
        await this.t._commit();
        let x1 = await Test.promise.findById(this.x.id);
        x1.t.should.eql(transaction.NULL_OBJECTID);
        x1.num.should.eql(2);
    });

    it('find fetch all documents of matched, ' +
        'they should finish commit process of previous transaction',
        async function() {
            this.x.num = 2;
            await this.t.add(this.x);
            let y = new Test({num: 1});
            await y.promise.save();
            y.num = 2;
            await this.t.add(y);
            await this.t._commit();
            let docs = await Test.promise.find({});
            should.exists(docs);
            docs.length.should.eql(2);
            docs.forEach((d) => {
                d.t.should.eql(transaction.NULL_OBJECTID);
                d.num.should.eql(2);
            });
        });

    it('findById fetch a document, ' +
        'it should cancel removed previous transaction',
        async function() {
            await Test.collection.promise.update(
                    {_id: this.x._id},
                    {$set: {t: new mongoose.Types.ObjectId()}}
            );
            let x1 = await Test.promise.findById(this.x._id);
            should.exists(x1);
            x1.t.should.eql(transaction.NULL_OBJECTID);
        });

    it('find fetch all documents of matched, ' +
        'they should cancel removed previous transaction',
        async function() {
            await Test.collection.promise.update(
                    {_id: this.x._id},
                    {$set: {t: new mongoose.Types.ObjectId()}}
            );
            let x2 = new Test({num: 2, t: new mongoose.Types.ObjectId()});
            await x2.promise.save();
            let xs = await Test.promise.find({});
            should.exists(xs);
            xs.length.should.eql(2);
            xs.forEach((x) => x.t.should.eql(transaction.NULL_OBJECTID));
        });

    it('findOne should wait previous transaction lock',
        async function() {
            await this.t.add(this.x);
            // var st = +new Date();
            try {
                await Test.promise.findOne({_id: this.x._id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
            // ((+new Date()) - st >= 37 * 5).should.be.true;
        });

    it('findOneNatvie fetch a native mongo document of matched, ' +
        'it should cancel removed previous transaction',
        async function() {
            let x = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            await x.promise.save();
            let x1 = await Test.promise.findOneNative({_id: x._id});
            should.exists(x1);
            x1.t.should.eql(transaction.NULL_OBJECTID);
        });

    it('findNatvie fetch all native mongo documents of matched, ' +
        'they should cancel removed previous transaction',
        async function() {
            let x1 = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            let x2 = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            await Promise.each([x1, x2], async(doc) => {
                await doc.promise.save();
            });
            let xs = await Test.promise.findNative({});
            should.exists(xs);
            let count = await xs.promise.count();
            should.exists(count);
            count.should.not.eql(0);
            xs.rewind();
            let xs1 = await xs.promise.toArray();
            should.exists(xs1);
            xs1.length.should.eql(count);
            xs1.forEach((x) => x.t.should.eql(transaction.NULL_OBJECTID));
        });

    it('can be force fetch document, ignoring transaction lock',
        async function() {
            await this.t.add(this.x);
            let x1 = await Test.promise.findOneForce({_id: this.x._id});
            should.exists(x1);
            x1.t.should.eql(this.t._id);
        });
});

describe('Find documents from transaction', function() {
    it('findOne fetch a document and automatic set transaction lock',
        async function() {
            let x1 = await this.t.findOne(Test, {_id: this.x._id});
            should.exist(x1);
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(x1.t);
            x1.num = 2;
            await this.t.commit();
            (await this.x.getNative()).num.should.eql(2);
        });

    // FIXME: findOne muse try to remove `t`
    xit('findOne fetch a document of matched, ' +
        'it should finish commit process of previous transaction',
        async function() {
            this.x.num = 2;
            await this.t.add(this.x);
            await this.t._commit();
            let t2 = await Transaction.begin();
            // FIXME: make TransactionSchema.findById
            // t2.findById(Test, x.id, sync.defer());
            let x1 = await t2.findOne(Test, {_id: this.x._id});
            should.exists(x1);
            x1.t.should.eql(transaction.NULL_OBJECTID);
            x1.num.should.eql(2);
        });

    it('find fetch documents and automatic set transaction lock',
        async function() {
            let docs = await this.t.find(Test, {_id: this.x._id});
            should.exist(docs);
            Array.isArray(docs).should.be.true;
            docs.length.should.be.eql(1);
            let x1 = docs[0];
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(x1.t);
            x1.num = 2;
            await this.t.commit();
            (await this.x.getNative()).num.should.eql(2);
        });

    it('Transaction.findOne should support sort option', async function() {
        await (new Test()).promise.save();
        await (new Test()).promise.save();
        let t1 = await this.t.findOne(Test, null, {sort: {'_id': 1}});
        should.exist(t1);
        let t2 = await this.t.findOne(Test, null, {sort: {'_id': -1}});
        should.exist(t2);
        t1._id.should.not.eql(t2._id);
    });

    // FIXME: current find only check t is `NULL_OBJECTID`
    // so, docs.length is always return 0
    xit('find fetch all documents of matched, ' +
        'they should finish commit process of previous transaction',
        async function() {
            this.x.num = 2;
            await this.t.add(this.x);
            let y = new Test({num: 1});
            y.promise.save();
            y.num = 2;
            await this.t.add(y);
            await this.t._commit();
            let t2 = await Transaction.begin();
            let docs = await t2.find(Test, {});
            should.exists(docs);
            docs.length.should.eql(2);
            docs.forEach((d) => {
                d.t.should.eql(transaction.NULL_OBJECTID);
                d.num.should.eql(2);
            });
        });

    // FIXME: current findOne not try post process, fail document fetch
    // also, we need test of document exist
    xit('findOne fetch a document of matched, ' +
        'it should finish commit process of previous transaction',
        async function() {
            let x = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            await x.promise.save();
            // t.findById(Test, x._id, sync.defer());
            let x1 = await this.t.findOne(Test, {_id: x._id});
            should.exists(x1);
            x1.t.should.eql(this.t._id);
        });

    // FIXME: current find only check t is `NULL_OBJECTID`
    // so, docs.length is always return 0
    xit('find fetch all documents of matched, ' +
        'they should cancel removed previous transaction',
        async function() {
            let x1 = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            let x2 = new Test({num: 2, t: new mongoose.Types.ObjectId()});
            await Promise.each([x1, x2], async(doc) => {
                await doc.promise.save();
            });
            let xs = await this.t.find(Test, {});
            xs.length.should.eql(2);
            xs.forEach((x) => x.t.should.eql(transaction.NULL_OBJECTID));
        });

    it('find fetch documents and automatic set transaction lock',
        async function() {
            let docs = await this.t.find(Test, {_id: this.x._id});
            should.exist(docs);
            Array.isArray(docs).should.be.true;
            docs.length.should.be.eql(1);
            let x1 = docs[0];
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(x1.t);
            x1.num = 2;
            await this.t.commit();
            (await this.x.getNative()).num.should.eql(2);
        });
});

describe('Transaction conflict', function() {
    it('above two transaction mark manage document mark at the same time',
        async function() {
            let t1 = await Transaction.begin();
            let x1 = await Test.promise.findById(this.x.id);
            this.x.num = 2;
            await this.t.add(this.x);
            x1.num = 3;
            try {
                await t1.add(x1);
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_1);
            }
        });

    it('already transacted document try save on another process',
        async function() {
            let x1 = await Test.promise.findById(this.x.id);
            this.x.num = 2;
            await this.t.add(this.x);
            x1.num = 3;
            try {
                await x1.promise.save();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_1);
            }
            await this.t.commit();
            let x2 = await this.x.getNative();
            x2.t.should.eql(transaction.NULL_OBJECTID);
            x2.num.should.eql(2);
        });

    it('(normal)not transacted document try save on another process',
        async function() {
            let x1 = await Test.promise.findOne({_id: this.x._id});
            x1.num = 2;
            await x1.promise.save();
            let x2 = await this.x.getNative();
            x2.t.should.eql(transaction.NULL_OBJECTID);
            x2.num.should.eql(2);
        });

    it.skip('(broken) we cannot care manually sequential update ' +
            'as fetched document without transaction', async function() {
        let x = new Test({num: 1});
        await x.promise.save();
        let x1 = await Test.promise.findOne({_id: x._id});
        let x2 = await Test.promise.findOne({_id: x._id});
        let t1 = await Transaction.begin();
        x1.num = 2;
        await t1.add(x1);
        await t1.commit();
        let t2 = await Transaction.begin();
        x2.num = 3;
        await t2.add(x2);
        should.fail('no error was thrown');
        await t2.commit();
    });

    it('findOne from transaction prevent race condition when fetch a document',
        async function() {
            let x1 = await this.t.findOne(Test, {_id: this.x._id});
            should.exist(x1);
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            try {
                await this.t.findOne(Test, {_id: this.x._id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        });
});

describe('Transaction lock', function() {
    it('model.findById should raise error at try fetch to locked document',
        async function() {
            this.x.num = 2;
            await this.t.add(this.x);
            try {
                await Test.promise.findById(this.x.id);
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        });

    // FIXME: wait lock feature should be default option of findOne
    xit('model.findOne should wait unlock previous transaction lock',
        async function() {
            let self = this;
            await self.t.add(self.x);

            let promise1 = (async() => {
                let x1 = await Test.promise.findOne({_id: self.x._id});
                should.exists(x1);
                x1.t.should.eql(transaction.NULL_OBJECTID);
            })();
            let promise2 = (async() => {
                await utils.sleep(100);
                await self.t.commit();
            })();

            await Promise.all([promise1, promise2]);
        });

    it('model.findOne should wait ' +
        'unlock previous transaction lock',
        async function() {
            let self = this;
            let x = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            await self.t.add(x);

            let promise1 = (async() => {
                let x1 = await Test.promise.findOne({_id: self.x._id});
                should.exists(x1);
                x1.t.should.eql(transaction.NULL_OBJECTID);
            })();

            let promise2 = (async() => {
                await utils.sleep(100);
                await self.t.commit();
            })();
            await Promise.all([promise1, promise2]);
        });

    // FIXME: current findOne not try post process, fail document fetch
    // also, we need test of document exist
    xit('transaction.findOne should raise error ' +
        'at try fetch to locked document',
        async function() {
            this.x.num = 2;
            await this.t.add(this.x);
            let t1 = await Transaction.begin();
            try {
                await t1.findOne(Test, {_id: this.x.id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        });

    it('transaction.findOne should raise error ' +
        'at try fetch to locked document ' +
        'and previous transaction was alive',
        async function() {
            await this.t.add(this.x);
            let t1 = await Transaction.begin();
            // let st = +new Date();
            try {
                await t1.findOne(Test, {_id: this.x._id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
            // ((+new Date()) - st >= 37 * 5).should.be.true;
        });

    it('transaction.findOne should wait unlock previous transaction lock',
        async function() {
            var self = this;
            let t1 = await Transaction.begin();
            await self.t.add(self.x);

            let promise1 = (async() => {
                let x1 = await t1.findOne(Test, {_id: self.x._id});
                should.exists(x1);
                x1.t.should.eql(t1._id);
            })();
            let promise2 = (async() => {
                await utils.sleep(100);
                await self.t.commit();
            })();
            await Promise.all([promise1, promise2]);
        });

    // FIXME: wait lock feature should be default option of findOne
    xit('transaction.findOne should wait unlock previous transaction lock',
        async function() {
            let self = this;
            let t1 = await Transaction.begin();
            await self.t.add(self.x);

            let promise1 = (async() => {
                let x1 = await t1.findOne(Test, {_id: self.x._id});
                should.exists(x1);
                x1.t.should.eql(transaction.NULL_OBJECTID);
            })();
            let promise2 = (async() => {
                await utils.sleep(100);
                await self.t.commit();
            })();

            await Promise.all([promise1, promise2]);
        });

    it('overtime transaction should expire automatically',
        async function() {
            let beforeGap =
                +new Date() - transaction.TRANSACTION_EXPIRE_GAP;
            let t = new Transaction({
                _id: mongoose.Types.ObjectId.createFromTime(beforeGap),
            });
            //            wrapTransactionMethods(t);
            let x = new Test({num: 1});
            await t.begin();
            await t.add(x);
            try {
                await t.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_EXPIRED);
            }
            should.not.exists(await x.getNative());
        });
});

describe('Transaction state conflict', function() {
    it('already committed transaction cannot move expire state',
        async function() {
            await this.t._commit();
            await this.t.expire();
            this.t.state.should.eql('commit');
        });

    it('already expired transaction cannot move commit state',
        async function() {
            await this.t._expire();
            try {
                await this.t.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_EXPIRED);
            }
            this.t.state.should.eql('expire');
        });

    it('if transaction expired for another process, cannot move commit state',
        async function() {
            let t1 = await Transaction.promise.findById(this.t._id);
            t1.state = 'expire';
            await t1.promise.save();
            try {
                await this.t.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.UNKNOWN_COMMIT_ERROR);
            }
            this.t.state.should.eql('expire');
        });

    it('if transaction committed for another process, ' +
        'cannot move expire state',
        async function() {
            let t1 = await Transaction.promise.findById(this.t._id);
            t1.state = 'commit';
            await t1.promise.save();
            try {
                await this.t.expire();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.SOMETHING_WRONG);
            }
            this.t.state.should.eql('expire');
        });

    it('if transaction committed for another process' +
        'we use persistent data of mongodb',
        async function() {
            await this.t.add(this.x);
            this.x.num = 3;
            let t1 = await Transaction.promise.findById(this.t._id);
            t1._docs = [];
            let y = new Test({num: 2});
            await t1.add(y);
            await t1._commit();
            await this.t.commit();
            let x1 = await this.x.getNative();
            should.exists(x1);
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            x1.num.should.eql(1);
            let y1 = await y.getNative();
            should.exists(y1);
            y1.t.should.eql(transaction.NULL_OBJECTID);
            y1.num.should.eql(2);
        });

    it('if mongodb raise error when transaction commit, ' +
        'automatically move to expire state', async function() {
        let self = this;
        let save = self.t._moveState;
        let called = false;
        self.t._moveState = async function(_, __) {
            if (!called) {
                called = true;
                throw new Error('something wrong');
            }
            return save.apply(self.t, arguments);
        };
        try {
            await self.t.commit();
            should.fail('no error was thrown');
        } catch (e) {
            e.message.should.eql(ERRORS.UNKNOWN_COMMIT_ERROR);
        }
        self.t.state.should.eql('expire');
    });
});
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
