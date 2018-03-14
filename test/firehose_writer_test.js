const Writer = require('../lib/firehose_writer')
const { Firehose } = require('aws-sdk')

describe('FirehoseWriter', function() {
  describe('constructor', function() {
    it('requires streamName', function() {
      expect(() => new Writer()).to.throw(Error, /streamName.*be specified/)
    })

    it('configures defaults', function() {
      const writer = new Writer({ streamName: 'something' })
      expect(writer.maxSize).to.eql(1)
      expect(writer.maxCount).to.eql(500)
      expect(writer.maxTimeout).to.eql(10000)

      expect(writer.maxBatchCount).to.eql(500)
      expect(writer.maxBatchSize).to.eql(1)

      expect(writer.firehoseClient).to.be.instanceOf(Firehose)
      expect(writer.maxRetries).to.eql(10)
    })

    it('applies provided options', function() {
      const writer = new Writer({
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
      expect(new Writer({ streamName: 'test' })._buffer).to.eql([])
    })
  })

  describe('put', function() {
    let writer
    beforeEach(function () {
      writer = new Writer({ streamName: 'test', maxSize: .001, maxCount: 100 })
      this.sinon.spy(writer, '_flush')
      // writer._flush = this.sinon.spy()
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
      writer = new Writer({ streamName: 'test', maxSize: 1, maxCount: 3 })

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
    let writer = new Writer({ streamName: 'test', maxBatchSize: 0.001, maxBatchCount: 3 })

    const SMALL_RECORD = Buffer.from('a', 'utf8')   // size: 1
    const MEDIUM_RECORD = Buffer.from('a'.repeat(500), 'utf-8')
    const HUGE_RECORD = Buffer.from('a'.repeat(1000000), 'utf-8')

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
})
