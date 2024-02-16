# Text to speech censoring benchmark

This benchmark requires an s3 bucket with the name `multi-x-serverless-text-2-speech-censoring`.

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run text_2_speech_censoring-version_number -a '{"message": "Some text with profanity, for example shit."}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove text_2_speech_censoring-version_number
```
