# Firehose writer

Provides a buffered queue abstraction for writing to AWS [Firehose](https://aws.amazon.com/kinesis/data-firehose/). Based on the original implementation by [firehose-writer](https://github.com/WiserSolutions/firehose-writer) but adjusted to work with the new AWS SDK V3 Firehose client.

Handles the following:

- Buffering
  - by time
  - by size
  - by count
- Retries

# Install

```sh
npm install @getdelta/firehose-writer
```

# Usage

```js
const FirehoseWriter = require('@getdelta/firehose-writer')

const writer = new FirehoseWriter({
  streamName: 'some-stream'// Name of the delivery stream

  // [Optional]
  // Specifies a log function to call
  // Callaback params:
  //    level: warn|error
  //    message: description of the error
  //    params: additional data to complement the error
  log: function(level, message, params) {
  }

  firehoseClient: ...,     // [Optional] override firehose client

  maxTimeout: 10000        // Flush records after 10 seconds, default: 10000ms (10 seconds)
  maxSize: 1               // Flush records after 1 Byte of data accumulated, default: 4 000 000
  maxCount: 400            // Flush records after 400 records buffered, default: 500 records

  maxRetries: 10           // Max delivery attempts for failed records/batches, default: 10 retries

  maxBatchCount: 500       // Max size of the delivery batch (cannot exceed 500). Default: 500
  maxBatchSize: 1000000    // Max batch size in bytes (can not exceed 4Mb). Default: 4 000 000
})

writer.put({ foo: 'bar' })
```
