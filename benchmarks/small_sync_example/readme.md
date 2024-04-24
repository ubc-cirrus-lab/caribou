# Regression Tuning Benchmarks

You can deploy the benchmark with the following command:

```bash
poetry run caribou deploy
```

And then run the benchmark with the following command:

```bash
poetry run caribou run small_sync_example-version_number -a '{"execute_1": true, "execute_2": false, "execute_1": true}'
```

Where the first two execute can be used to invoke the first sync function and the third execute can be used to invoke the second sync function.

The logic of whether a sync node runs is the following:

| execute_1 | execute_2 | execute_3 | sync_function runs | second_sync_function runs |
|-----------|-----------|-----------|--------------------|---------------------------|
| false     | false     | false     | false              | false                     |
| true      | false     | false     | true               | true                      |
| false     | true      | false     | true               | true                      |
| false     | false     | true      | false              | true                      |
| true      | true      | false     | true               | true                      |
| true      | false     | true      | true               | true                      |
| false     | true      | true      | true               | true                      |
| true      | true      | true      | true               | true                      |

To remove the benchmark, you can use the following command:

```bash
poetry run caribou remove small_sync_example-version_number
```
