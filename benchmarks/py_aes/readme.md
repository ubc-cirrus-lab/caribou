# PyAES Benchmarks

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run py_aes-version_number -a '{"length_of_message": 100, "num_of_iterations": 50}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove py_aes-version_number
```
