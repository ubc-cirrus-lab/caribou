# RAG Data Ingestion Benchmark

This benchmark is the `Data Ingestion` part of the bigger document chat application 
benchmark: [https://github.com/UBC-CIC/document-chat](https://github.com/UBC-CIC/document-chat) (the original repository's license file is included in this directory)

This benchmark requires access to the S3 bucket named `caribou-document-embedding-benchmark`,
a valid `PostgreSQL` database with its credential saved to `AWS Secret Manager`, two dynamoDB 
tables by the name of `caribou-document-embedding-benchmark-document` (Partition key: `userid` (String),
Sort key: `documentid` (String)) and `caribou-document-embedding-benchmark-memory` 
(Partition key: SessionId (String)), and permission to use the `Titan Text Embeddings V2` model in AWS Bedrock. 
All with the AWS Region set to or located in `us-east-1` (N. Virginia).

Alternatively, the user should set the aforementioned databases and dependencies in the `app.py`.

You can also enable/disable image extraction from images by changing the EXTRACT_IMAGES macro.

There needs to be a file in the S3 bucket, for example, `example.pdf`, in a folder called `input`,
or any valid PDF file.

This benchmark allows for usage of custom AWS Bedrock Embedding, which
can be configured with an input argument `embedding_model_id` 
(https://docs.aws.amazon.com/bedrock/latest/userguide/titan-embedding-models.html).
If this is not set, this benchmark defaults to using `amazon.titan-embed-text-v2:0`.

You can deploy the benchmark with the following command while inside the poetry environment:

```bash
caribou deploy
```

And then run the benchmark with the following command:

```bash
caribou run rag_data_ingestion-version_number -a '{"user_id": "example_user_1", "file_name": "example.pdf"}'
```

To remove the benchmark, you can use the following command:

```bash
caribou remove rag_data_ingestion-version_number
```
