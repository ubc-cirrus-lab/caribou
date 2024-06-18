# DNA Visualization benchmark

This benchmark requires access to the S3 bucket named `caribou-dna-visualization`,
with the AWS Region set to `us-west-2` (Oregon).

Alternatively, the user may change the S3 bucket name in `app.py` and the associated `iam_policy.json`, 
and also modify all instances of `region_name='us-west-2'` to match the region of the bucket.

There needs to be a file in the bucket named `small_sequence.gb` in a folder called `genbank`, 
or a valid DNA sequence input from https://www.ncbi.nlm.nih.gov/genbank/.

You can deploy the benchmark with the following command:

```bash		
poetry run caribou deploy	
```
```bash
poetry run caribou run dna_visualization-version_number -a '{"gen_file_name": "small_sequence.gb"}'
```

To remove the benchmark, you can use the following command:
```bash
poetry run caribou remove dna_visualization-version_number
```