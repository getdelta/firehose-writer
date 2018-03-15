const MEGABYTE = 1000000

function createClient() {
  const { Firehose } = require('aws-sdk')
  return new Firehose()
}

function delay(timeout) {
  return new Promise(function(resolve) { setTimeout(resolve, timeout) })
}

module.exports = class FirehoseWriter {
  /**
  * @constructor
  *
  * @param {Object} options
  * @param {integer} options.maxSize
  * @param {integer} options.maxCount
  * @param {integer} options.maxTimeout
  *
  * @param {integer} options.maxRetries
  *
  * @param {AWS.Firehose} options.firehoseClient
  */
  constructor(options = {}) {
    if (!options) throw new Error('`options` can not be `null` or `undefined`')

    if (!options.streamName) throw new Error('`streamName` should be specified')
    this.streamName = options.streamName
    this.firehoseClient = options.firehoseClient || createClient()

    this.maxSize = options.maxSize || 1
    this.maxCount = options.maxCount || 500
    this.maxTimeout = options.maxTimeout || 10000

    this.maxRetries = options.maxRetries || 10
    this.retryInterval = options.retryInterval || 5000

    this.maxBatchCount = options.maxBatchCount || 500
    this.maxBatchSize = options.maxBatchSize || 1

    this._buffer = []
    this._currentSize = 0

    this.interval = setInterval(() => this._flush(), this.maxTimeout)
  }

  /**
  * @param {Object|Buffer} record - Object or Buffer to send to firehose
  */
  put(record) {
    if (!record) throw new Error('`record` must be provided an can not be null or undefined')
    if (typeof record === 'string') throw new Error('strings not supported')

    const data = record instanceof Buffer ?
      record :
      Buffer.from(JSON.stringify(record), 'utf8')

    this._buffer.push(data)
    this._currentSize += data.length

    if (this._oneOfThresholdsReached()) {
      this._flush()
    }
  }

  _oneOfThresholdsReached() {
    return this._currentSize >= this.maxSize * MEGABYTE ||
      this._buffer.length >= this.maxCount
  }

  async _flush() {
    if (!this._buffer || !this._buffer.length) return

    const chunks = this._splitIntoChunks(this._buffer)
    this._buffer = []
    this._currentSize = 0

    return Promise.all(chunks.map(chunk => this._deliver(chunk.records)))
  }

  async _deliver(records, retry = 0) {
    if (retry > this.maxRetries) {
      throw new Error(`Failed to deliver a batch of ${records.length} to firehose stream ${this.streamName} (${this.maxRetries} retries)`)
    }

    const params = {
      DeliveryStreamName: this.streamName,
      Records: records.map(r => ({ Data: r.toString('base64') }))
    }
    return this.firehoseClient.putRecordBatch(params).promise()
      .then((response) => this._retryFailedRecords(records, response, retry + 1))
      .catch(() =>
        delay(this.retryInterval).then(() => this._deliver(records, retry + 1))
      )
  }

  async _retryFailedRecords(originalRecords, response, retry = 0) {
    const failedRecords = response.RequestResponses
      .map((x, i) => x.ErrorCode && originalRecords[i])
      .filter(Boolean)
    if (failedRecords && failedRecords.length) {
      // If all records fail wait for the interval
      if (failedRecords.length === originalRecords.length) await delay(this.retryInterval)

      await this._deliver(failedRecords, retry)
    }
  }

  _splitIntoChunks(records) {
    return records.reduce((acc, record) => {
      const recordSize = record.length

      let chunk = acc[0]
      const sizeFits = chunk.size + recordSize <= this.maxBatchSize * MEGABYTE
      const countFits = chunk.records.length + 1 <= this.maxBatchCount
      if (sizeFits && countFits) {
        chunk.records.push(record)
        chunk.size += recordSize
      } else {
        acc.splice(0, 0, { records: [record], size: recordSize })
      }
      return acc
    }, [{ records: [], size: 0 }])
  }
}
