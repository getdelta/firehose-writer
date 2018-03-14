# Firehose writer

Provides a buffered queue abstraction for writing to AWS [Firehose](https://aws.amazon.com/kinesis/data-firehose/).

Handles the following:
- Buffering
  - by time
  - by size
  - by count
- Retries

# Install

```sh
npm install firehose-writer
```

# Usage

```js
const FirehoseWriter = require('firehose-writer')

const writer = new FirehoseWriter({
  streamName: 'some-stream'// Name of the delivery stream

  firehoseClient: ...,     // [Optional] override firehose client

  maxTimeout: 10000,       // Flush records after 10 seconds, default: 10 seconds
  maxSize: 1,              // Flush records after 1 Mb of data accumulated, default: 1Mb
  maxCount: 400            // Flush records after 400 records buffered, default: 500 records

  maxRetries: 10           // Max delivery attempts for failed records/batches, default: 10 retries

  maxBatchCount: 500       // Max size of the delivery batch (cannot exceed 500). Default: 500
  maxBatchSize: 1          // Max batch size in MB (can not exceed 1Mb). Default: 1Mb
})

writer.put({ foo: 'bar' })
```
