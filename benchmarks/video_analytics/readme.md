# Video analytics benchmark

Original source: [https://github.com/vhive-serverless/vSwarm](https://github.com/vhive-serverless/vSwarm) (the original repository's license file is included in this directory)

This benchmark requires access to the S3 bucket named `caribou-video-analytics`,
with the AWS Region set to `us-east-1` (N. Virginia).

Alternatively, the user may change the S3 bucket name and region in `app.py`,
by changing the values of `s3_bucket_name` and `s3_bucket_region_name` to the
desired bucket.

There needs to be an video file in the bucket, for example `video.mp4`, in a folder
called `input`.

This benchmark allow for custom recognition stage fan-out which
can be configured with an input argument `fanout_num` between `1-6`, or `-1` for
auto fan-out. If this is not set, it defaults to -1 or automatic.

This benchmark allow for a custom output folder name which can be configured with
an input argument `output_folder_name` which will determine where in the `output` folder
it will be stored as. If this is not set, it defaults to the name of the input text file
without the file extension.

You can deploy the benchmark with the following command while inside the poetry environment:

```bash
poetry run caribou deploy
```

And then run the benchmark with the following command:

```bash
poetry run caribou run video_analytics-version_number -a '{"video_name": "video.mp4", "fanout_num": 6}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run caribou remove video_analytics-version_number
```
