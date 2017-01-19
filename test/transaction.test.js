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

const mochaAsync = (fn) => {
    return (done) => {
        fn.call().then(done).catch(done);
    };
};

before(mochaAsync(async() => {
    await initialize.promise();

    Test = transaction.TransactedModel(connection, 'Test', TestSchema);
    // FIXME: need init process
    transaction.TransactionSchema.plugin(
        transaction.bindShardKeyRule,
        {
            fields: {shard: {type: Number, required: true}},
            rule: {shard: 1, _id: 1},
            initialize: (doc) => {
                doc.shard = doc.shard || doc._id.getTimestamp().getTime();
            },
        }
    );
    Transaction = connection.model(transaction.TRANSACTION_COLLECTION,
                                   transaction.TransactionSchema);
    transaction.addCollectionPseudoModelPair(
        Transaction.collection.name, connection,
        transaction.TransactionSchema
    );
}));

beforeEach(function() {
    this.timeout(10000);
});

afterEach((done) => {
    if (!connection || !connection.db) {
        return done();
    }
    connection.db.dropDatabase(done);
});

const createTestSet = async() => {
    const x = new Test({num: 1});
    await x.promise.save();
    const t = await Transaction.begin();
    return [x, t];
};

describe('TransactedModel', function() {
    it('should have transaction lock at create new doucment',
        mochaAsync(async() => {
            const [x] = await createTestSet();
            x.t.should.eql(transaction.NULL_OBJECTID);
        }));

    it('should have transaction lock at fetch document from database',
        mochaAsync(async function() {
            const [x] = await createTestSet();
            const doc = await Test.promise.findById(x._id);
            doc.t.should.eql(transaction.NULL_OBJECTID);
        }));

    it('should fetch lock and sharding fields if not exists at fetch targets',
        mochaAsync(async function() {
            const [x] = await createTestSet();
            const test = await Test.promise.findById(x._id, 'num');
            should.exists(test.t);
            should.exists(test._id);
        }));

    it('result of toJSON should remove lock field',
        mochaAsync(async function() {
            const [x] = await createTestSet();
            const doc = await Test.promise.findById(x._id);
            doc.toJSON().should.not.have.property('t');
        }));

    it('can be try fetch non exist document', mochaAsync(async function() {
        const id = new mongoose.Types.ObjectId();
        const doc = await Test.promise.findOne({_id: id});
        should.not.exists(doc);
    }));
});

describe('Save with transaction', function() {
    it('transaction add should check validate schema',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            x.num = 10;
            try {
                await t.add(x);
                should.fail('no error was thrown');
            } catch (e) {
                e.name.should.eql('ValidationError');
            }
        }));

    it('update can be possible', mochaAsync(async function() {
        const [x, t] = await createTestSet();
        x.num = 2;
        await t.add(x);
        const x1 = await x.getNative();
        t._id.should.eql(x1.t);
        x1.num.should.eql(1);
        x1.def.should.eql(1);
        await t.commit();
        const x2 = await x.getNative();
        x2.t.should.eql(transaction.NULL_OBJECTID);
        x2.num.should.eql(2);
        should.not.exists(await t.getNative());
    }));

    it('can make new document', mochaAsync(async function() {
        const [, t] = await createTestSet();
        const x = new Test({num: 1});
        await t.add(x);
        const x1 = await x.getNative();
        t._id.should.eql(x1.t);
        should.not.exist(x1.num);
        x1.__new.should.eql(true);
        await t.commit();
        const x2 = await x.getNative();
        x2.t.should.eql(transaction.NULL_OBJECTID);
        x2.num.should.eql(1);
        x2.def.should.eql(1);
        should.not.exists(x2.__new);
        should.not.exists(await t.getNative());
    }));

    it('if cancel transaction process and contains new documents, ' +
        'should cancel make new documents',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            const x = new Test({num: 1});
            await t.add(x);
            const x1 = await x.getNative();
            should.exists(x1);
            t._id.should.eql(x1.t);
            should.not.exist(x1.num);
            try {
                await t.cancel('testcase');
            } catch (e) {}
            should.not.exists(await x.getNative());
            should.not.exists(await t.getNative());
        }));

    it('if stop in the middle of transaction process,' +
            'should cancel make new documents',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            const x = new Test({num: 1});
            await t.add(x);
            await t.remove();
            const xs = await Test.promise.find({_id: x._id});
            should.exists(xs);
            xs.length.should.eql(0);
        }));

    it('should support multiple documents with transaction',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            x.num = 2;
            await t.add(x);
            const y = new Test({string: 'abcd'});
            await t.add(y);
            await t.commit();
            (await x.getNative()).num.should.eql(2);
            (await y.getNative()).string.should.eql('abcd');
            should.not.exists(await t.getNative());
        }));

    it('should support remove document with transaction',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            await t.removeDoc(x);
            await t.commit();
            should.not.exists(await x.getNative());
        }));

    it('if cancel transaction process, also cancel reserved remove document',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            await t.removeDoc(x);
            await t.expire();
            should.exists(await x.getNative());
        }));
});

describe('Find documents from model', function() {
    it('auto commit before load data', mochaAsync(async function() {
        const [x, t] = await createTestSet();
        x.num = 2;
        await t.add(x);
        await t._commit();
        const x1 = await Test.promise.findById(x.id);
        x1.t.should.eql(transaction.NULL_OBJECTID);
        x1.num.should.eql(2);
    }));

    it('find fetch all documents of matched, ' +
        'they should finish commit process of previous transaction',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            x.num = 2;
            await t.add(x);
            const y = new Test({num: 1});
            await y.promise.save();
            y.num = 2;
            await t.add(y);
            await t._commit();
            const docs = await Test.promise.find({});
            should.exists(docs);
            docs.length.should.eql(2);
            docs.forEach((d) => {
                d.t.should.eql(transaction.NULL_OBJECTID);
                d.num.should.eql(2);
            });
        }));

    it('findById fetch a document, ' +
        'it should cancel removed previous transaction',
        mochaAsync(async function() {
            const [x] = await createTestSet();
            await Test.collection.promise.update(
                    {_id: x._id},
                    {$set: {t: new mongoose.Types.ObjectId()}}
            );
            const x1 = await Test.promise.findById(x._id);
            should.exists(x1);
            x1.t.should.eql(transaction.NULL_OBJECTID);
        }));

    it('find fetch all documents of matched, ' +
        'they should cancel removed previous transaction',
        mochaAsync(async function() {
            const [x] = await createTestSet();
            await Test.collection.promise.update(
                    {_id: x._id},
                    {$set: {t: new mongoose.Types.ObjectId()}}
            );
            const x2 = new Test({num: 2, t: new mongoose.Types.ObjectId()});
            await x2.promise.save();
            const xs = await Test.promise.find({});
            should.exists(xs);
            xs.length.should.eql(2);
            xs.forEach((x) => x.t.should.eql(transaction.NULL_OBJECTID));
        }));

    it('findOne should wait previous transaction lock',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            await t.add(x);
            // var st = +new Date();
            try {
                await Test.promise.findOne({_id: x._id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
            // ((+new Date()) - st >= 37 * 5).should.be.true;
        }));

    it('findOneNatvie fetch a native mongo document of matched, ' +
        'it should cancel removed previous transaction',
        mochaAsync(async function() {
            const x = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            await x.promise.save();
            const x1 = await Test.promise.findOneNative({_id: x._id});
            should.exists(x1);
            x1.t.should.eql(transaction.NULL_OBJECTID);
        }));

    it('findNatvie fetch all native mongo documents of matched, ' +
        'they should cancel removed previous transaction',
        mochaAsync(async function() {
            const x1 = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            const x2 = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            await Promise.each([x1, x2], async(doc) => {
                await doc.promise.save();
            });
            const xs = await Test.promise.findNative({});
            should.exists(xs);
            const count = await xs.promise.count();
            should.exists(count);
            count.should.not.eql(0);
            xs.rewind();
            const xs1 = await xs.promise.toArray();
            should.exists(xs1);
            xs1.length.should.eql(count);
            xs1.forEach((x) => x.t.should.eql(transaction.NULL_OBJECTID));
        }));

    it('can be force fetch document, ignoring transaction lock',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            await t.add(x);
            const x1 = await Test.promise.findOneForce({_id: x._id});
            should.exists(x1);
            x1.t.should.eql(t._id);
        }));
});

describe('Find documents from transaction', function() {
    it('findOne fetch a document and automatic set transaction lock',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            const x1 = await t.findOne(Test, {_id: x._id});
            should.exist(x1);
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(x1.t);
            x1.num = 2;
            await t.commit();
            (await x.getNative()).num.should.eql(2);
        }));

    // FIXME: findOne muse try to remove `t`
    xit('findOne fetch a document of matched, ' +
        'it should finish commit process of previous transaction',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            x.num = 2;
            await t.add(x);
            await t._commit();
            const t2 = await Transaction.begin();
            // FIXME: make TransactionSchema.findById
            // t2.findById(Test, x.id, sync.defer());
            const x1 = await t2.findOne(Test, {_id: x._id});
            should.exists(x1);
            x1.t.should.eql(transaction.NULL_OBJECTID);
            x1.num.should.eql(2);
        }));

    it('find fetch documents and automatic set transaction lock',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            const docs = await t.find(Test, {_id: x._id});
            should.exist(docs);
            Array.isArray(docs).should.be.true;
            docs.length.should.be.eql(1);
            const x1 = docs[0];
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(x1.t);
            x1.num = 2;
            await t.commit();
            (await x.getNative()).num.should.eql(2);
        }));

    it('Transaction.findOne should support sort option',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            await (new Test()).promise.save();
            await (new Test()).promise.save();
            const t1 = await t.findOne(Test, null, {sort: {'_id': 1}});
            should.exist(t1);
            const t2 = await t.findOne(Test, null, {sort: {'_id': -1}});
            should.exist(t2);
            t1._id.should.not.eql(t2._id);
        }));

    // FIXME: current find only check t is `NULL_OBJECTID`
    // so, docs.length is always return 0
    xit('find fetch all documents of matched, ' +
        'they should finish commit process of previous transaction',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            x.num = 2;
            await t.add(x);
            const y = new Test({num: 1});
            y.promise.save();
            y.num = 2;
            await t.add(y);
            await t._commit();
            const t2 = await Transaction.begin();
            const docs = await t2.find(Test, {});
            should.exists(docs);
            docs.length.should.eql(2);
            docs.forEach((d) => {
                d.t.should.eql(transaction.NULL_OBJECTID);
                d.num.should.eql(2);
            });
        }));

    // FIXME: current findOne not try post process, fail document fetch
    // also, we need test of document exist
    xit('findOne fetch a document of matched, ' +
        'it should finish commit process of previous transaction',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            const x = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            await x.promise.save();
            // t.findById(Test, x._id, sync.defer());
            const x1 = await t.findOne(Test, {_id: x._id});
            should.exists(x1);
            x1.t.should.eql(t._id);
        }));

    // FIXME: current find only check t is `NULL_OBJECTID`
    // so, docs.length is always return 0
    xit('find fetch all documents of matched, ' +
        'they should cancel removed previous transaction',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            const x1 = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            const x2 = new Test({num: 2, t: new mongoose.Types.ObjectId()});
            await Promise.each([x1, x2], async(doc) => {
                await doc.promise.save();
            });
            const xs = await t.find(Test, {});
            xs.length.should.eql(2);
            xs.forEach((x) => x.t.should.eql(transaction.NULL_OBJECTID));
        }));

    it('find fetch documents and automatic set transaction lock',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            const docs = await t.find(Test, {_id: x._id});
            should.exist(docs);
            Array.isArray(docs).should.be.true;
            docs.length.should.be.eql(1);
            const x1 = docs[0];
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            should.exist(x1.t);
            x1.num = 2;
            await t.commit();
            (await x.getNative()).num.should.eql(2);
        }));
});

describe('Transaction conflict', function() {
    it('above two transaction mark manage document mark at the same time',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            const t1 = await Transaction.begin();
            const x1 = await Test.promise.findById(x.id);
            x.num = 2;
            await t.add(x);
            x1.num = 3;
            try {
                await t1.add(x1);
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_1);
            }
        }));

    it('already transacted document try save on another process',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            const x1 = await Test.promise.findById(x.id);
            x.num = 2;
            await t.add(x);
            x1.num = 3;
            try {
                await x1.promise.save();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_1);
            }
            await t.commit();
            const x2 = await x.getNative();
            x2.t.should.eql(transaction.NULL_OBJECTID);
            x2.num.should.eql(2);
        }));

    it('(normal)not transacted document try save on another process',
        mochaAsync(async function() {
            const [x] = await createTestSet();
            const x1 = await Test.promise.findOne({_id: x._id});
            x1.num = 2;
            await x1.promise.save();
            const x2 = await x.getNative();
            x2.t.should.eql(transaction.NULL_OBJECTID);
            x2.num.should.eql(2);
        }));

    it.skip('(broken) we cannot care manually sequential update ' +
            'as fetched document without transaction',
        mochaAsync(async function() {
            const x = new Test({num: 1});
            await x.promise.save();
            const x1 = await Test.promise.findOne({_id: x._id});
            const x2 = await Test.promise.findOne({_id: x._id});
            const t1 = await Transaction.begin();
            x1.num = 2;
            await t1.add(x1);
            await t1.commit();
            const t2 = await Transaction.begin();
            x2.num = 3;
            await t2.add(x2);
            should.fail('no error was thrown');
            await t2.commit();
        }));

    it('findOne from transaction prevent race condition when fetch a document',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            const x1 = await t.findOne(Test, {_id: x._id});
            should.exist(x1);
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            try {
                await t.findOne(Test, {_id: x._id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        }));
});

describe('Transaction lock', function() {
    it('model.findById should raise error at try fetch to locked document',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            x.num = 2;
            await t.add(x);
            try {
                await Test.promise.findById(x.id);
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        }));

    // FIXME: wait lock feature should be default option of findOne
    xit('model.findOne should wait unlock previous transaction lock',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            await t.add(x);

            const promise1 = (async() => {
                const x1 = await Test.promise.findOne({_id: x._id});
                should.exists(x1);
                x1.t.should.eql(transaction.NULL_OBJECTID);
            })();
            const promise2 = (async() => {
                await utils.sleep(100);
                await t.commit();
            })();

            await Promise.all([promise1, promise2]);
        }));

    it('model.findOne should wait ' +
        'unlock previous transaction lock',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            const x1 = new Test({num: 1, t: new mongoose.Types.ObjectId()});
            await t.add(x1);

            const promise1 = (async() => {
                const x2 = await Test.promise.findOne({_id: x._id});
                should.exists(x2);
                x2.t.should.eql(transaction.NULL_OBJECTID);
            })();

            const promise2 = (async() => {
                await utils.sleep(100);
                await t.commit();
            })();
            await Promise.all([promise1, promise2]);
        }));

    // FIXME: current findOne not try post process, fail document fetch
    // also, we need test of document exist
    xit('transaction.findOne should raise error ' +
        'at try fetch to locked document',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            x.num = 2;
            await t.add(x);
            const t1 = await Transaction.begin();
            try {
                await t1.findOne(Test, {_id: x.id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
        }));

    it('transaction.findOne should raise error ' +
        'at try fetch to locked document ' +
        'and previous transaction was alive',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            await t.add(x);
            const t1 = await Transaction.begin();
            // const st = +new Date();
            try {
                await t1.findOne(Test, {_id: x._id});
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_CONFLICT_2);
            }
            // ((+new Date()) - st >= 37 * 5).should.be.true;
        }));

    it('transaction.findOne should wait unlock previous transaction lock',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            const t1 = await Transaction.begin();
            await t.add(x);

            const promise1 = (async() => {
                const x1 = await t1.findOne(Test, {_id: x._id});
                should.exists(x1);
                x1.t.should.eql(t1._id);
            })();
            const promise2 = (async() => {
                await utils.sleep(100);
                await t.commit();
            })();
            await Promise.all([promise1, promise2]);
        }));

    // FIXME: wait lock feature should be default option of findOne
    xit('transaction.findOne should wait unlock previous transaction lock',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            const t1 = await Transaction.begin();
            await t.add(x);

            const promise1 = (async() => {
                const x1 = await t1.findOne(Test, {_id: x._id});
                should.exists(x1);
                x1.t.should.eql(transaction.NULL_OBJECTID);
            })();
            const promise2 = (async() => {
                await utils.sleep(100);
                await t.commit();
            })();

            await Promise.all([promise1, promise2]);
        }));

    it('overtime transaction should expire automatically',
        mochaAsync(async function() {
            const beforeGap =
                +new Date() - transaction.TRANSACTION_EXPIRE_GAP;
            const t = new Transaction({
                _id: mongoose.Types.ObjectId.createFromTime(beforeGap),
            });
            //            wrapTransactionMethods(t);
            const x = new Test({num: 1});
            await t.begin();
            await t.add(x);
            try {
                await t.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_EXPIRED);
            }
            should.not.exists(await x.getNative());
        }));
});

describe('Transaction state conflict', function() {
    it('already committed transaction cannot move expire state',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            await t._commit();
            await t.expire();
            t.state.should.eql('commit');
        }));

    it('already expired transaction cannot move commit state',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            await t._expire();
            try {
                await t.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.TRANSACTION_EXPIRED);
            }
            t.state.should.eql('expire');
        }));

    it('if transaction expired for another process, cannot move commit state',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            const t1 = await Transaction.promise.findById(t._id);
            t1.state = 'expire';
            await t1.promise.save();
            try {
                await t.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.UNKNOWN_COMMIT_ERROR);
            }
            t.state.should.eql('expire');
        }));

    it('if transaction committed for another process, ' +
        'cannot move expire state',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            const t1 = await Transaction.promise.findById(t._id);
            t1.state = 'commit';
            await t1.promise.save();
            try {
                await t.expire();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.SOMETHING_WRONG);
            }
            t.state.should.eql('expire');
        }));

    it('if transaction committed for another process' +
        'we use persistent data of mongodb',
        mochaAsync(async function() {
            const [x, t] = await createTestSet();
            await t.add(x);
            x.num = 3;
            const t1 = await Transaction.promise.findById(t._id);
            t1._docs = [];
            const y = new Test({num: 2});
            await t1.add(y);
            await t1._commit();
            await t.commit();
            const x1 = await x.getNative();
            should.exists(x1);
            x1.t.should.not.eql(transaction.NULL_OBJECTID);
            x1.num.should.eql(1);
            const y1 = await y.getNative();
            should.exists(y1);
            y1.t.should.eql(transaction.NULL_OBJECTID);
            y1.num.should.eql(2);
        }));

    it('if mongodb raise error when transaction commit, ' +
            'automatically move to expire state',
        mochaAsync(async function() {
            const [, t] = await createTestSet();
            const save = t._moveState;
            let called = false;
            t._moveState = async function(_, __) {
                if (!called) {
                    called = true;
                    throw new Error('something wrong');
                }
                return save.apply(t, arguments);
            };
            try {
                await t.commit();
                should.fail('no error was thrown');
            } catch (e) {
                e.message.should.eql(ERRORS.UNKNOWN_COMMIT_ERROR);
            }
            t.state.should.eql('expire');
        }));
});
// vim: et ts=4 sw=4 sts=4 colorcolumn=80
