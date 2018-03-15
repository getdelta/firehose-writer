const Writer = require('../lib/firehose_writer')
const { Firehose } = require('aws-sdk')
const sinon = require('sinon')

const SMALL_RECORD = Buffer.from('a', 'utf8')   // size: 1
const MEDIUM_RECORD = Buffer.from('a'.repeat(500), 'utf-8')
const HUGE_RECORD = Buffer.from('a'.repeat(1000000), 'utf-8')

describe('FirehoseWriter', function() {
  function newWriter(options, stubDeliverMethod = true) {
    const defaults = { streamName: 'test' }
    const writer = new Writer({...defaults, ...options})

    if (stubDeliverMethod) sinon.stub(writer, '_deliver')

    return writer
  }

  describe('constructor', function() {
    it('requires streamName', function() {
      expect(() => new Writer()).to.throw(Error, /streamName.*be specified/)
    })

    it('configures defaults', function() {
      const writer = newWriter()
      expect(writer.maxSize).to.eql(1)
      expect(writer.maxCount).to.eql(500)
      expect(writer.maxTimeout).to.eql(10000)

      expect(writer.maxBatchCount).to.eql(500)
      expect(writer.maxBatchSize).to.eql(1)

      expect(writer.firehoseClient).to.be.instanceOf(Firehose)
      expect(writer.maxRetries).to.eql(10)
    })

    it('applies provided options', function() {
      const writer = newWriter({
        streamName: 'someStream',
        firehoseClient: 'hello',
        maxCount: 1,
        maxSize: 2,
        maxRetries: 3,
        maxTimeout: 4,
        maxBatchCount: 5,
        maxBatchSize: 6
      })
      expect(writer.streamName).to.equal('someStream')
      expect(writer.firehoseClient).to.equal('hello')
      expect(writer.maxCount).to.eql(1)
      expect(writer.maxSize).to.eql(2)
      expect(writer.maxRetries).to.eql(3)
      expect(writer.maxTimeout).to.eql(4)
      expect(writer.maxBatchCount).to.eql(5)
      expect(writer.maxBatchSize).to.eql(6)
    })

    it('initializes the buffer', function() {
      expect(newWriter()._buffer).to.eql([])
    })
  })

  describe('put', function() {
    let writer
    beforeEach(function () {
      writer = newWriter({ maxSize: .001, maxCount: 100 })
      this.sinon.spy(writer, '_flush')
    })

    it('adds a record to the buffer', function() {
      writer.put({ a: 1 })
      expect(writer._buffer.length).to.eql(1)
    })

    it('requires a record', function() {
      expect(() => writer.put()).to.throw(Error)
    })

    it('updates _currentSize', function() {
      writer.put({ a: 1 })      // {"a":1}
      const buffer = Buffer.from('{"a":1}', 'utf8')
      expect(writer._currentSize).to.eql(buffer.length)
    })

    it('does not support strings', function() {
      expect(() => writer.put('hello')).to.throw(Error, 'strings not supported')
    })

    it('supports buffers', function() {
      writer.put(Buffer.from('hello', 'utf8'))
      expect(writer._currentSize).to.eql(5)
    })

    describe('limit reached', function() {
      it('calls _flush on size limit reach', function() {
        writer._currentSize = 999
        writer.put(Buffer.from('a', 'utf8'))
        expect(writer._flush).to.have.been.called
      })

      it('calls _flush on records count limit reached', function() {
        for (var i = 0; i < 99; i++) writer._buffer.push('dummyrecord')
        writer.put({ a: 1 })
        expect(writer._flush).to.have.been.called
      })
    })
  })

  describe('_flush', function() {
    let writer
    beforeEach(function() {
      writer = newWriter({ maxSize: 1, maxCount: 3 })

      writer.put({ a: 1 })
      writer.put({ b: 2 })
      expect(writer._buffer.length).to.eql(2)
      expect(writer._currentSize).to.eql(14)
    })

    it('returns a promise', function() {
      expect(writer._flush()).to.be.instanceOf(Promise)
    })

    it('resets all counters and buffer', function() {
      writer._flush()
      expect(writer._buffer).to.eql([])
      expect(writer._currentSize).to.eql(0)
    })
  })

  describe('_splitIntoChunks', function() {
    // maxSize: 1000 bytes
    let writer = newWriter({ maxBatchSize: 0.001, maxBatchCount: 3 })

    function bufferOf(record, times) {
      const buffer = []
      for (let i = 0; i < times; i++) buffer.push(record)
      return buffer
    }

    it('tries to put everything into one chunk', function() {
      expect(writer._splitIntoChunks([SMALL_RECORD, SMALL_RECORD]))
        .to.eql([
          { records: [SMALL_RECORD, SMALL_RECORD], size: 2 }
        ])
    })

    it('splits records by size', function() {
      expect(writer._splitIntoChunks([MEDIUM_RECORD, MEDIUM_RECORD, SMALL_RECORD]))
        .to.eql([
          { records: [SMALL_RECORD], size: 1 },
          { records: [MEDIUM_RECORD, MEDIUM_RECORD], size: 1000}
        ])
    })

    it('splits records by count', function() {
      const chunks = writer._splitIntoChunks(bufferOf(SMALL_RECORD, 4))
      expect(chunks)
        .to.eql([
          { records: bufferOf(SMALL_RECORD, 1), size: 1 },
          { records: bufferOf(SMALL_RECORD, 3), size: 3 }
        ])
    })
  })

  describe('_deliver', function() {
    const AWSMock = require('aws-sdk-mock')
    const records = [SMALL_RECORD, MEDIUM_RECORD]
    let Firehose, writer, putRecordBatchStub = () => {}

    beforeEach(function() {
      AWSMock.setSDKInstance(require('aws-sdk'))
      AWSMock.mock('Firehose', 'putRecordBatch', function(params, cb) {
        putRecordBatchStub(...arguments)
      })
      Firehose = require('aws-sdk').Firehose
      writer = newWriter({ firehoseClient: new Firehose() }, false)
    })
    afterEach(function() { AWSMock.restore() })

    it('calls putRecordBatch', async function() {
      putRecordBatchStub = this.sinon.stub().callsFake((params, cb) => {
        cb(null, { RequestResponses: [] })
      })

      await writer._deliver(records)
      expect(putRecordBatchStub).to.have.been.called
    })

    it('redelivers failed records', async function() {
      writer.retryInterval = 10000

      putRecordBatchStub = this.sinon.stub().callsFake((_, cb) => {
        cb(null, { RequestResponses: [
          { ErrorCode: 0 },
          { ErrorCode: 1 }
        ] })
      })
      const start = Date.now()
      await writer._deliver(records)
      expect(Date.now() - start).to.be
        .lte(writer.retryInterval / 2, 'Record delivery failure should not wait retryInterval')
      expect(putRecordBatchStub).to.have.been.calledWithMatch(
        this.sinon.match((params) => params.Records.length === 1)
      )
    })

    it('redelivers batch on general failure', async function() {
      writer.retryInterval = 20
      putRecordBatchStub = this.sinon.stub().callsFake((_, cb) => cb(new Error('Delivery error')))

      const start = Date.now()
      await expect(writer._deliver(records)).to.be.rejectedWith(/Failed to deliver a batch of 2 to firehose stream test \(10 retries\)/)
      expect(Date.now() - start).to.be
        .gte(writer.retryInterval * 10, 'General failure should wait for retryInterval')
      expect(putRecordBatchStub).to.have.been.calledWithMatch(
        this.sinon.match((params) => params.Records.length === 2)
      ).callCount(11)
    })
  })
})
