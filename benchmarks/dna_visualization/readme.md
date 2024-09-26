# DNA Visualization benchmark

Original source: [https://github.com/ddps-lab/serverless-faas-workbench](https://github.com/spcl/serverless-benchmarks/) (the original repository's license file is included in this directory).
Source for the adapted version used by us: [https://github.com/ubc-cirrus-lab/unfaasener](https://github.com/ubc-cirrus-lab/unfaasener)

This benchmark requires access to the S3 bucket named `caribou-dna-visualization`,
with the AWS Region set to `us-east-1` (N. Virginia).

Alternatively, the user may change the S3 bucket name and region in `app.py`,
by changing the values of `s3_bucket_name` and `s3_bucket_region_name` to the
desired bucket.

There needs to be a file in the bucket, for example `sequence.gb`, in a folder called `genbank`,
or a valid DNA sequence input from <https://www.ncbi.nlm.nih.gov/genbank/>.

You can deploy the benchmark with the following command while inside the poetry environment:

```bash
caribou deploy
```

And then run the benchmark with the following command:

```bash
caribou run dna_visualization-version_number -a '{"gen_file_name": "sequence.gb"}'
```

To remove the benchmark, you can use the following command:

```bash
caribou remove dna_visualization-version_number
```
