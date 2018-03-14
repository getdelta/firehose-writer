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
  firehoseClient: ...,     // [Optional] override firehose client

  maxTimeout: 10000,       // Flush records after 10 seconds
  maxSize: 1,              // Flush records after 1 Mb of data accumulated
  maxCount: 400            // Flush records after 400 records buffered

  maxRetries: 10           // Max delivery attempts for failed records/batches
})

writer.put({ foo: 'bar' })
```
