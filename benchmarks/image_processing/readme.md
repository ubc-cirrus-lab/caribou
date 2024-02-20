# Image Processing Benchmarks

This benchmark requires access to the s3 bucket with the name `multi-x-serverless-image-processing-benchmark`. There needs to be an image in the bucket with some name.

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run image_processing-version_number -a '{"message": "image_name.jpeg"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove image_processing-version_number
```
