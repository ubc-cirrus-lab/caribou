# Image Processing Benchmarks

This benchmark requires access to the s3 bucket with the name `caribou-image-processing-benchmark`. There needs to be an image in the bucket with some name.

You can deploy the benchmark with the following command:

```bash
poetry run caribou deploy
```

And then run the benchmark with the following command:

```bash
poetry run caribou run image_processing-version_number -a '{"image_name": "image_name.jpeg"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run caribou remove image_processing-version_number
```
