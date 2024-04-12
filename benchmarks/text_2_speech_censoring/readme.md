# Text to speech censoring benchmark

This benchmark requires access to the s3 bucket with the name `multi-x-serverless-text-2-speech-censoring`.

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run text_2_speech_censoring-version_number -a '{"input_file": "input_file.txt"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove text_2_speech_censoring-version_number
```
