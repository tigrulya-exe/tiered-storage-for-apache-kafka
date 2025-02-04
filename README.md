# Tiered Storage for Apache Kafka®

This project is an implementation of `RemoteStorageManager` for Apache Kafka tiered storage.

The implementation will have multiple configurable storage backends. Currently, AWS S3, Google Cloud Storage, and Azure Blob Storage are supported.

The project follows the API specifications according to the latest version of [KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/x/KJDQBQ).

## Getting started

See the [demo/](demo/) directory.

## Design

### Requirements

Conceptually, `RemoteStorageManagers` is very simple and exposes a CRUD interface for uploading, (ranged) fetching, and deleting Kafka log segment files. This implementation was done with few additional requirements:
1. Support for AWS S3, Google Cloud Storage, and Azure Blob Storage.
2. Compression. It must provide optional (configurable) compression. The compression must be conditional and double compression of Kafka-compressed log segments should be avoided.
3. Encryption. It must provide optional (configurable) encryption with key rotation support.
4. Small download overhead. It must avoid wasted download from remote storage.

### Storage backends

There's little difference in how data is written to and read from different cloud object storages, mostly in using different SDKs. This implementation abstracts away the concept of storage backend. This allows to select the storage backend with configuration.

### Chunking

Compression and encryption make it difficult to do ranged queries. To read an encrypted file from the middle, one needs to decompress / decrypt--and so download--the whole prefix. As files can be large, this may lead to huge download overhead.

One way to combat this is chunking. With chunking, the original file is split into chunks, which are transformed (compressed, encrypted) independently.

```
Original file:               Transformed file:
+-----------+ -------\
|           |         \
|  Chunk 1  |          \----- +-----------+
|           |                 |  Chunk 1  |
+-----------+ --------------- +-----------+
|           |                 |  Chunk 2  |
|  Chunk 2  |        /------- +-----------+
|           |       /         |  Chunk 3  |
+-----------+ -----/     /--- +-----------+
|  Chunk 3  |           /
+-----------+ ---------/
```

Now, knowing the sizes of original and transformed chunks, it's possible to map from a position in the original file `Po` to a position in the transformed file `Pt` within the accuracy of a chunk size. Thus, some overhead remains, but it is reduced as much as chunks are smaller than the whole file.

#### Concatenation and indexing

Splitting a file into multiple chunks may significantly increase the overhead of remote storage operations--most notably writes and lists--which are usually billed for. To avoid this, transformed chunks could be concatenated back into a single blob and uploaded as one.

To maintain the ability to map offsets/chunks between original and transformed files, an index is needed.

The index must be stored on the remote storage along with other files related to a log segment to keep these uploads self-sufficient.

It's important to keep the size of these indices small. One technique is to keep the size of original chunks fixed and explicitly list only the transformed side of chunks. Furthermore, if compression is not used, the size of transformed chunks also remains fixed.

##### Index encoding

Binary encoding could reduce the index size further. Values are encoded as a difference from a common base and the encoding uses the minimum number of bytes per value. See the [javadoc](java/io/aiven/kafka/tiered/storage/commons/chunkindex/serde/ChunkSizesBinaryCodec.java) for `ChunkSizesBinaryCodec` for details. We could expect 1-2 bytes per value on average.

A 2 GB file chunked into 1 MB chunks produce about 2000 index entries, which is 2-4 KB.

The index could be compressed with Zstd after that and have to be Base64-encoded (about 30% overhead). This results in 2.7-3.7 KB.

This encoding scheme allows indices to be stored (cached) in memory and accessed with little overhead.

**Rejected alternative**: An alternative approach may be to just write values as integers (4 byte per value) and rely on Zstd to compress it significantly. Despite this produces similar results size-wise (3.6-4.2 KB, i.e. a little worse), it's difficult to store indices in memory in the encoded form, because this would require decompression on each access.

### Compression

Optional data compression with Zstandard is supported. It can be disabled, enabled, or enabled conditionally based on a heuristic.

#### Compression heuristic

For each segment that is to be uploaded, the heuristic checks if the first batch in the segment is compressed. If it is not compressed, the whole segment is considered not compressed and compression is performed.

#### On compression dictionaries

It's possible to further reduce the size of compressed chunks if, instead of compressing them independently, first pretrain a shared compression dictionary. This is not implemented now and will potentially be addressed in the future: preliminary research didn't demonstrate significant advantage.

### Encryption

Optional client-side encryption is supported. Encryption is implemented with the envelope scheme with the key ring.

```
+----------------+                 +----------------+                 +-----------+
|      key       |                 |      data      |                 |  segment  |
| encryption key | ---encrypts---> | encryption key | ---encrypts---> |   data    |
+----------------+                 +----------------+                 +-----------+ 
```

Each segment has its unique AES-256 data encryption key (DEK). The segment data and indices are encrypted with this key. The DEK itself is encrypted with the public RSA key encryption key (KEK). The KEK ID is stored together with the encrypted DEK so that the corresponding KEK (the private one) can be found on the key ring for decryption.

The key ring may contain several KEKs. One of them is active (used for encrypting DEKs during upload).

With this approach, it's possible to start using a new KEK for new data: keep the old KEKs on the key ring (for decrypting old data) and add the new KEK for new data. It's also possible to rotate the old KEKs. For this, the new KEK needs to be added to the key ring and then old DEKs must be re-encrypted with the new KEK and segment manifests updated. This can be done gradually and transparently.

### Uploads

#### S3 Multipart Upload

When uploading processed segments and indexes, multipart upload is used to put files on S3 back-end.

Even though, multipart transactions are aborted when an exception happens while processing, there's a chance that initiated transactions are not completed or aborted (e.g. broker process is killed) and incomplete part uploads hang without completing a transaction.
For these scenarios, is recommended to set a bucket lifecycle policy to periodically abort incomplete multipart uploads: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html>

### Ranged queries

TBD

### Local cache

Even in case of sequential reads, chunks may be required multiple times in a short period of time. Fetching them each time from the remote storage would negatively affect the latency. To mitigate this, the chunk caching is supported. There are two built-in implementations: the in-memory chunk cache and disk-based chunk cache. In the most cases, the former cannot be used in production.


## License

The project is licensed under the Apache license, version 2.0. Full license test is available in the [LICENSE](LICENSE) file.

## Contributing

For a detailed guide on how to contribute to the project, please check [CONTRIBUTING.md](CONTRIBUTING.md).

## Code of conduct

This project uses the [Contributor Covenant](https://www.contributor-covenant.org/) code of conduct. Please check [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for further details.

## Trademark notices

Apache Kafka is either a registered trademark or trademark of the Apache Software Foundation in the United States and/or other countries.
All product and service names used in this page are for identification purposes only and do not imply endorsement.

## Credits

This project is maintained by, [Aiven](https://aiven.io/) open source developers.

Recent contributors are listed on the GitHub project page, <https://github.com/aiven/tiered-storage-for-apache-kafka/graphs/contributors>.

Copyright (c) 2022 Aiven Oy and project contributors.
