# RAG Data Ingestion Benchmark

This benchmark is the `Data Ingestion` part of the bigger document chat application 
benchamrk: https://github.com/UBC-CIC/document-chat/

This benchmark requires access to the S3 bucket named `caribou-document-embedding-benchmark`,
a valid `postgresql` database with its credential saved to `AWS Secret Manager`, two dynamoDB 
tables by the name of `caribou-document-embedding-benchmark-document` () and 
`caribou-document-embedding-benchmark-memory` (), and 
permission to use `Titan Text Embeddings V2` model in AWS Bedrock. 
all with the AWS Region set to or located in `us-east-1` (N. Virginia).

Alternatively, the user should set the affortmentioned databases and dependencies in the `app.py`.

There needs to be a file in the S3 bucket, for example `example.pdf`, in a folder called `input`,
or any valid PDF file.

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
