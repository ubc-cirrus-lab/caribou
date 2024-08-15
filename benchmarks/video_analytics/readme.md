# Video analytics benchmark

This benchmark requires access to the s3 bucket with the name `caribou-video-analytics`.

You can deploy the benchmark with the following command:

```bash
poetry run caribou deploy
```

And then run the benchmark with the following command:

```bash
poetry run caribou run video_analytics-version_number -a '{"video_name": "video_name.mp4"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run caribou remove video_analytics-version_number
```
