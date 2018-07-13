import {
  Writable,
  WritableOptions,
} from 'stream'

import { Kinesis } from 'aws-sdk'

export class KinesisWritable extends Writable {
  private debug: boolean

  public constructor(
    private kinesis: Kinesis,
    private streamName: string,
    private partitionKey: string,
    options: WritableOptions,
  ) {
    super(options)

    this.debug = false
  }

  enableDebug() {
    this.debug = true
  }

  async _write(chunk, encoding, next) {
    if (Buffer.isBuffer(chunk)) {
      chunk = chunk.toString(encoding)
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
    const records = chunks.map(({ chunk, encoding }) => {
      if (Buffer.isBuffer(chunk)) {
        chunk = chunk.toString(encoding)
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
