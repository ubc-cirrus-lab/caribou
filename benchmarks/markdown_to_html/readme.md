# Markdown to HTML Benchmark

Source for the adapted version used by us: https://github.com/PrincetonUniversity/faas-profiler/blob/master/functions/markdown-to-html/markdown2html.py

The markdown_to_html benchmark converts a input Markdown file to HTML. It uses an S3 bucket for input and output file storage and is deployed using the Caribou framework.

The benchmark requires access to an S3 bucket named `markdown_to_html` with the AWS Region set to `us-east-1`. Alternatively, users can specify a different bucket name and region by modifying `s3_bucket_name` and `s3_bucket_region_name` in the app.py file.

There needs to be a markdown file in the bucket.

You can deploy the benchmark with the following command while inside the poetry environment:
```
caribou deploy
```

And then run the benchmark with the following command:
```
caribou run markdown_to_html-version-number -a '{"filename": "file.md"}'
```

To remove the benchmark, you can use the following command:
```
caribou remove markdown_to_html-version-number
```