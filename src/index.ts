import {
  Writable,
  WritableOptions,
} from 'stream'

import { Kinesis } from 'aws-sdk'

export interface KinesisWritableOptions {
  debug?: boolean
  kinesis: Kinesis
  streamName: string
  partitionKey: string
}

export class KinesisWritable extends Writable {
  private debug: boolean
  private kinesis: Kinesis
  private streamName: string
  private partitionKey: string

  public constructor(
    options: KinesisWritableOptions,
    writableOptions?: WritableOptions,
  ) {
    super(writableOptions)

    Object.assign(this, { debug: false }, options)
  }

  async _write(chunk, _encoding, next) {
    if (Buffer.isBuffer(chunk)) {
      chunk = chunk.toString()
    }

    const params = {
      Data: chunk,
      StreamName: this.streamName,
      PartitionKey: this.partitionKey,
    }

    try {
      const response = await this.kinesis.putRecord(params).promise()

      if (this.debug) {
        console.log('response:', JSON.stringify(response, null, 2))
      }

      next()
    } catch (error) {
      next(error)
    }
  }

  async _writev(chunks, next) {
    const records = chunks.map(({ chunk }) => {
      if (Buffer.isBuffer(chunk)) {
        chunk = chunk.toString()
      }

      return {
        Data: chunk,
        PartitionKey: this.partitionKey,
      }
    })
    const params = {
      Records: records,
      StreamName: this.streamName,
    }

    try {
      const response = await this.kinesis.putRecords(params).promise()

      if (this.debug) {
        console.log('response:', JSON.stringify(response, null, 2))
      }

      next()
    } catch (error) {
      next(error)
    }
  }
}
