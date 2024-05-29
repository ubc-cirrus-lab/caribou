# Text to speech censoring benchmark

This benchmark requires access to the s3 bucket with the name `caribou-text-2-speech-censoring`,
with the AWS Region set to `us-west-2` (Oregon).

Alternatively, the user may change the S3 bucket name in `app.py` and the associated `iam_policy.json`, 
and also modify all instances of `region_name='us-west-2'` to match the region of the bucket.

There needs to be an text file in the bucket with some name in a folder called `input`.

You can deploy the benchmark with the following command:

```bash
poetry run caribou deploy
```

And then run the benchmark with the following command:

```bash
poetry run caribou run text_2_speech_censoring-version_number -a '{"input_file": "input_file.txt"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run caribou remove text_2_speech_censoring-version_number
```
