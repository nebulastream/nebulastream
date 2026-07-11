---
type: sink
name: S3Sink
plugin: true
---

## Description

A sink that uploads formatted records to Amazon S3. Records are accumulated in memory across multiple
`execute()` calls and flushed as a single batch object upload when `stop()` is called or when the
batch size threshold is reached.

## Config Parameters

- BUCKET (string, required): Name of the S3 bucket to upload to
- PREFIX (string, optional, default=): Key prefix prepended to every uploaded object name
- REGION (string, optional, default=us-east-1): AWS region where the bucket is located
- BATCH_SIZE (size, optional, default=1000): Number of records to accumulate before flushing
- ENDPOINT_URL (string, optional, default=): Custom endpoint URL for S3-compatible storage (e.g. MinIO)

## Behavior

`start()`: Initialize the AWS SDK S3 client using BUCKET, REGION, and optionally ENDPOINT_URL. Allocate the in-memory accumulation buffer. Throw `InvalidConfigParameter` if the bucket is not accessible.

`execute()`: Append the formatted bytes from the TupleBuffer to the accumulation buffer. When the accumulated record count reaches BATCH_SIZE, flush the buffer to S3 as a new object and reset.

`stop()`: Flush any remaining accumulated records to S3 as a final partial-batch object. Shut down the S3 client.

## Examples

- 1000 records accumulated → flushed to s3://BUCKET/PREFIX<timestamp>.csv
- Final 42 records at stop() → flushed as partial batch s3://BUCKET/PREFIX<timestamp>_final.csv
