# Text to speech censoring benchmark

This benchmark requires access to the s3 bucket with the name `multi-x-serverless-dna-visualization`.

There needs to be a file in the bucket with the name `sequence.gb` in a folder called `genbank`.

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run dna_visualization-version_number -a '{"gen_file_name": "sequence.gb"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove dna_visualization-version_number
```
