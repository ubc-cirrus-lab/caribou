# Image Processing Benchmarks

This benchmark requires access to the s3 bucket with the name `caribou-image-processing-benchmark`,
with the AWS Region set to `us-west-2` (Oregon).

Alternatively, the user may change the S3 bucket name in `app.py` and the associated `iam_policy.json`, 
and also modify all instances of `region_name='us-west-2'` to match the region of the bucket.

There needs to be an image in the bucket with some name in a folder called `input`.

You can deploy the benchmark with the following command:

```bash
poetry run caribou deploy
```

And then run the benchmark with the following command:

```bash
poetry run caribou run image_processing-0.0.1 -a '{"image_name": "downsized_best_painting.jpg"}'
poetry run caribou run image_processing-0.0.1 -a '{"image_name": "best_painting.jpg"}'

poetry run caribou run image_processing-version_number -a '{"image_name": "best_painting.jpg"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run caribou remove image_processing-version_number
```
