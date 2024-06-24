# Map Reduce Benchmarks

This benchmark requires access to the s3 bucket with the name `caribou-map-reduce`,
with the AWS Region set to `us-west-2` (Oregon).

Alternatively, the user may change the S3 bucket name in `app.py` and the associated `iam_policy.json`, 
and also modify all instances of `region_name='us-west-2'` to match the region of the bucket.

There needs to be a folder inside the input folder `input` with the base dir 
(`subset_256MB` in our example) containing the input file split in shards.
The input file should be split in shards of the same size.
The number of shards should be specified in the `number_shards` parameter.
The input file size should be specified in the `input_file_size` parameter and is used by the workflow to determine the number of workers.

You can deploy the benchmark with the following command:

```bash
poetry run caribou deploy
```

And then run the benchmark with the following command (example):

```bash
poetry run caribou run map_reduce-0.0.1 -a '{"input_base_dir": "subset_25_6MB", "number_shards": 120, "input_file_size": 26843545}'
poetry run caribou run map_reduce-0.0.1 -a '{"input_base_dir": "subset_256MB", "number_shards": 120, "input_file_size": 268435456}'

poetry run caribou run map_reduce-version_number -a '{"input_base_dir": "subset_256MB", "number_shards": 120, "input_file_size": 268435456}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run caribou remove map_reduce-version_number
```

## Data preparation

We based the experiment on the [Yelp Dataset](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset).

To generate subsets of the dataset, you can use the following command:

```bash
dd if=yelp_academic_dataset_review.json  of=subset_1024MB.txt bs=1M count=1024
```

This command will generate a file of 1024MB, for different sizes you can change the `count` parameter.

To then split the file in shards, you can use the following script:

```bash
mkdir -p chunks && split -n 120 -a 3 subset_1024MB.txt subset_1024MB/chunk_

cd chunks
a=0
for f in chunk_*; do
  mv "$f" "chunk_${a}.txt"
  let a=a+1
done
cd ..
```

This directory can then be directly uploaded to the s3 bucket to the `input` subdirectory.
