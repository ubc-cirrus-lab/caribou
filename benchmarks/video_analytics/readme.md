# Video analytics benchmark

This benchmark requires access to the s3 bucket with the name `multi-x-serverless-video-analytics`.

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run video_analytics-version_number -a '{"message": "input/video_name.mp4"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove video_analytics-version_number
```
