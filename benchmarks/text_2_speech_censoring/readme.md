# Text to speech censoring benchmark

This benchmark requires access to the s3 bucket with the name `caribou-text-2-speech-censoring`.

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
