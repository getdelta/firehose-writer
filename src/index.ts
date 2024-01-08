import {
  Firehose,
  PutRecordBatchCommandOutput,
} from '@aws-sdk/client-firehose';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { Agent } from 'https';

const AWS_MAX_BATCH_RECORDS = 500;
const AWS_MAX_BATCH_SIZE = 4 * 1000000;

function defaultLog(level: string, message: string, ...params: any[]) {
  console.log(`${level}: ${message}`, params);
}

function createClient() {
  return new Firehose({
    region: 'eu-west-1',
    requestHandler: new NodeHttpHandler({
      httpsAgent: new Agent({
        keepAlive: true,
      }),
    }),
  });
}

function delay(timeout: number) {
  return new Promise(function (resolve) {
    setTimeout(resolve, timeout);
  });
}

function validateParam(paramName: string, condition: boolean, message: string) {
  if (!condition) {
    throw new Error(`${paramName}: ${message}`);
  }
}

export interface Options {
  streamName?: string;
  maxSize?: number;
  maxCount?: number;
  maxTimeout?: number;
  maxRetries?: number;
  retryInterval?: number;
  maxBatchCount?: number;
  maxBatchSize?: number;
  firehoseClient?: any;
  log?: ((level: string, message: string, ...params: any[]) => void) | null;
}

export default class FirehoseWriter {
  public _buffer: any[];
  public _currentSize: number;
  public streamName?: string;
  public firehoseClient: Firehose;
  public maxSize: number;
  public maxCount: number;
  public maxTimeout: number;
  public maxRetries: number;
  public retryInterval: number;
  public maxBatchCount: number;
  public maxBatchSize: number;
  public log: (level: string, message: string, ...params: any[]) => void;

  constructor(options: Options = {}) {
    validateParam('options', !!options, 'can not be `null` or `undefined`');
    validateParam('streamName', !!options?.streamName, 'should be specified');

    this.streamName = options?.streamName;
    this.firehoseClient = options?.firehoseClient || createClient();
    this.log = options?.log || defaultLog;
    this._buffer = [];
    this._currentSize = 0;
    this.maxTimeout = options?.maxTimeout || 10000;
    this.maxSize = options?.maxSize || AWS_MAX_BATCH_SIZE;
    this.maxBatchCount = options?.maxBatchCount || AWS_MAX_BATCH_RECORDS;
    this.maxBatchSize = options?.maxBatchSize || AWS_MAX_BATCH_SIZE;
    this.retryInterval = options?.retryInterval || 5000;
    this.maxCount = options?.maxCount || 500;
    this.maxRetries = options?.maxRetries || 10;

    setInterval(() => this._flush(), this.maxTimeout).unref();
    process.on('beforeExit', () => this._flush());
  }

  public put(record: Buffer | any) {
    const data =
      record instanceof Buffer
        ? record
        : Buffer.from(JSON.stringify(record), 'utf8');

    const maxSizeBytes = this.maxSize;
    if (data.length > maxSizeBytes) {
      throw new Error(`Record bigger then max size (${maxSizeBytes})`);
    }

    this._buffer.push(data);
    this._currentSize += data.length;

    if (this._oneOfThresholdsReached()) {
      this._flush();
    }
  }

  private _oneOfThresholdsReached() {
    return (
      this._currentSize >= this.maxSize || this._buffer.length >= this.maxCount
    );
  }

  public async _flush() {
    if (!this._buffer || !this._buffer.length) {
      return;
    }

    const chunks = this._splitIntoChunks(this._buffer);
    this._buffer = [];
    this._currentSize = 0;

    return await Promise.all(
      chunks.map((chunk) => this._deliver(chunk.records)),
    );
  }

  public async _deliver(records: Uint8Array[], retry = 0) {
    if (retry > this.maxRetries) {
      throw new Error(
        `Failed to deliver a batch of ${records.length} to firehose stream ${this.streamName} (${this.maxRetries} retries)`,
      );
    }

    const params = {
      DeliveryStreamName: this.streamName,
      Records: records.map((r) => ({ Data: r })),
    };

    try {
      const response = await this.firehoseClient.putRecordBatch(params);
      this._retryFailedRecords(records, response, retry + 1);
    } catch (err) {
      this._handleGeneralFailure(records, err, retry + 1);
    }
  }

  private async _handleGeneralFailure(
    originalRecords: Uint8Array[],
    err: any,
    retry = 0,
  ) {
    const canRetry = !err || err.retryable;

    this.log('error', 'General failure in sending to firehose', {
      err,
      canRetry,
    });

    if (canRetry) {
      return await delay(this.retryInterval).then(() =>
        this._deliver(originalRecords, retry),
      );
    }
  }

  private async _retryFailedRecords(
    originalRecords: Uint8Array[],
    response: PutRecordBatchCommandOutput,
    retry = 0,
  ) {
    const failedRecords = response
      .RequestResponses!.map((x, i) => {
        if (!x.ErrorCode) {
          return;
        }

        return originalRecords[i];
      })
      .filter((x): x is Uint8Array => Boolean(x));

    if (failedRecords && failedRecords.length) {
      this.log('warn', 'Retrying failed records', {
        count: failedRecords.length,
      });

      // If all records fail wait for the interval
      if (failedRecords.length === originalRecords.length)
        await delay(this.retryInterval);

      await this._deliver(failedRecords, retry);
    }
  }

  public _splitIntoChunks(records: Uint8Array[]) {
    return records.reduce(
      (acc, record) => {
        const recordSize = record.length;

        const chunk = acc[0];
        const sizeFits = chunk.size + recordSize <= this.maxBatchSize;
        const countFits = chunk.records.length + 1 <= this.maxBatchCount;
        if (sizeFits && countFits) {
          chunk.records.push(record);
          chunk.size += recordSize;
        } else {
          acc.splice(0, 0, { records: [record], size: recordSize });
        }

        return acc;
      },
      [{ records: [], size: 0 } as { records: Uint8Array[]; size: number }],
    );
  }
}
