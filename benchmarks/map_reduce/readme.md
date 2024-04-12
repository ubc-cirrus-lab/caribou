# Map Reduce Benchmarks

This benchmark requires access to the s3 bucket with the name `multi-x-serverless-map-reduce`. There needs to be an image in the bucket with some name.

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command (example):

```bash
poetry run multi_x_serverless run map_reduce-0.0.1 -a '{"input_base_dir": "subset_256MB", "number_shards": 120, "input_file_size": 268435456}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove map_reduce-version_number
```
