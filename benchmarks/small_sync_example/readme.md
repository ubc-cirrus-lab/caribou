# Regression Tuning Benchmarks

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run small_sync_example-version_number -a '{"execute_1": true, "execute_2": false, "execute_1": true}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove small_sync_example-version_number
```
