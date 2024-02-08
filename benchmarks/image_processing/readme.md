# Image Processing Benchmarks

This benchmark requires an s3 bucket with the name `multi-x-serverless-image-processing-benchmark`. There needs to be an image in the bucket with some name.

You can deploy the benchmark with the following command:

```bash
poetry run multi-x-serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi-x-serverless run image_processing-version_number -i '{"message": "image_name.jpeg"}'
```
