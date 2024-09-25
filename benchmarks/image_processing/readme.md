# Image Processing Benchmark

Original source: https://github.com/ddps-lab/serverless-faas-workbench (the original repository's license file is included in this directory).

This benchmark requires access to the S3 bucket named `caribou-image-processing-benchmark`,
with the AWS Region set to `us-east-1` (N. Virginia).

Alternatively, the user may change the S3 bucket name and region in `app.py`,
by changing the values of `s3_bucket_name` and `s3_bucket_region_name` to the
desired bucket.

There needs to be an image file in the bucket, for example `image_name.jpg`, in a folder
called `input`.

This benchmark requires setting a list of between 1 to 5 desired transformations
in the input argument `desired_transformations`, of the following available options
: `flip`, `rotate`, `blur`, `greyscale`, and or `resize`.

You can deploy the benchmark with the following command while inside the poetry environment:

```bash
caribou deploy
```

And then run the benchmark with the following command:

```bash
caribou run image_processing-version_number -a '{"image_name": "image_name.jpg", "desired_transformations": ["flip", "rotate", "blur", "greyscale", "resize"]}}'
```

To remove the benchmark, you can use the following command:

```bash
caribou remove image_processing-version_number
```
