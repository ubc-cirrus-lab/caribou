# Map Reduce Benchmarks

This benchmark requires access to the s3 bucket with the name `multi-x-serverless-map-reduce`. There needs to be an image in the bucket with some name.

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run map_reduce-version_number -a '{"input_file": "text_file_name"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove map_reduce-version_number
```
