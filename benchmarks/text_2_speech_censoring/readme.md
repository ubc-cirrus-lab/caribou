# Text to speech censoring benchmark

Original source: [https://github.com/SimonEismann/FunctionsAndWorkflows](https://github.com/SimonEismann/FunctionsAndWorkflows) (the original repository's license file is included in this directory)

This benchmark requires access to the S3 bucket named `caribou-text-2-speech-censoring`,
with the AWS Region set to `us-east-1` (N. Virginia).

Alternatively, the user may change the S3 bucket name and region in `app.py`, by changing the values of `s3_bucket_name` and `s3_bucket_region_name` to the desired bucket.

There needs to be an text file in the bucket, for example `text_file.txt`, in a folder called `input`.

This benchmark allow for usage of either `AWS Polly` or `Google Text-To-Speech` which
can be configured with an input argument `t2s_service` of either `polly` or `gtts`.
If this is not set, this benchark defaults to using `AWS Polly`.

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
poetry run caribou run text_2_speech_censoring-version_number -a '{"input_file": "text_file.txt", "t2s_service": "polly"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run caribou remove text_2_speech_censoring-version_number
```
