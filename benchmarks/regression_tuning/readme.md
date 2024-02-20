# Regression Tuning Benchmarks

This benchmark requires access to the s3 bucket with the name `multi-x-serverless-regression-tuning`.

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run regression_tuning-version_number -a '{"message": 10}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove regression_tuning-version_number
```
