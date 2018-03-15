AWS_MAX_RECORD_SIZE = 1024000
AWS_MAX_BATCH_RECORDS = 500
AWS_MAX_BATCH_SIZE = AWS_MAX_BATCH_RECORDS * AWS_MAX_RECORD_SIZE

function defaultLog(level, message, params) {
  console.log(`${level}: ${message}`, params)
}

function createClient() {
  const { Firehose } = require('aws-sdk')
  return new Firehose()
}

function delay(timeout) {
  return new Promise(function(resolve) { setTimeout(resolve, timeout) })
}

function validateParam(paramName, condition, message) {
  if (!condition) {
    throw new Error(`${paramName}: ${message}`)
  }
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
    validateParam('options', !!options, 'can not be `null` or `undefined`')
    validateParam('streamName', !!options.streamName, 'should be specified')
    
    this.streamName = options.streamName
    this.firehoseClient = options.firehoseClient || createClient()

    this.log = options.log || defaultLog

    this.maxSize = options.maxSize || 100000000
    validateParam('maxSize', this.maxSize <= AWS_MAX_BATCH_SIZE, `should be at most ${AWS_MAX_BATCH_SIZE}`)

    this.maxCount = options.maxCount || 500
    validateParam('maxCount', this.maxCount <= AWS_MAX_BATCH_RECORDS, `should be at most ${AWS_MAX_BATCH_RECORDS}`)

    this.maxTimeout = options.maxTimeout || 10000

    this.maxRetries = options.maxRetries || 10
    this.retryInterval = options.retryInterval || 5000

    this.maxBatchCount = options.maxBatchCount || 500
    validateParam('maxBatchCount', this.maxBatchCount <= AWS_MAX_BATCH_RECORDS, `should be at most ${AWS_MAX_BATCH_RECORDS}`)

    this.maxBatchSize = options.maxBatchSize || AWS_MAX_RECORD_SIZE * AWS_MAX_BATCH_RECORDS
    validateParam('maxBatchSize', this.maxBatchSize <= AWS_MAX_BATCH_SIZE, `should be at most ${AWS_MAX_BATCH_SIZE}`)

    this._buffer = []
    this._currentSize = 0

    setInterval(() => this._flush(), this.maxTimeout).unref()
    process.on('beforeExit', () => this._flush())
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

    const maxSizeBytes = this.maxSize
    if (data.length > maxSizeBytes) {
      throw new Error(`Record bigger then max size (${maxSizeBytes})`)
    }

    this._buffer.push(data)
    this._currentSize += data.length

    if (this._oneOfThresholdsReached()) {
      this._flush()
    }
  }

  _oneOfThresholdsReached() {
    return this._currentSize >= this.maxSize ||
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
      Records: records.map(r => ({ Data: r }))
    }
    return this.firehoseClient.putRecordBatch(params).promise()
      .then((response) => this._retryFailedRecords(records, response, retry + 1))
      .catch(err => this._handleGeneralFailure(records, err, retry + 1))
  }

  async _handleGeneralFailure(originalRecords, err, retry = 0) {
    const canRetry = !err || err.retryable

    this.log('error', 'General failure in sending to firehose', { err, canRetry })

    if (canRetry) {
      return delay(this.retryInterval).then(() => this._deliver(originalRecords, retry))
    }
  }

  async _retryFailedRecords(originalRecords, response, retry = 0) {
    const failedRecords = response.RequestResponses
      .map((x, i) => x.ErrorCode && originalRecords[i])
      .filter(Boolean)

    if (failedRecords && failedRecords.length) {
      this.log('warn', 'Retrying failed records', { count: failedRecords.length })

      // If all records fail wait for the interval
      if (failedRecords.length === originalRecords.length) await delay(this.retryInterval)

      await this._deliver(failedRecords, retry)
    }
  }

  _splitIntoChunks(records) {
    return records.reduce((acc, record) => {
      const recordSize = record.length

      let chunk = acc[0]
      const sizeFits = chunk.size + recordSize <= this.maxBatchSize
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
