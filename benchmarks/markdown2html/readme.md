# Markdown to HTML Benchmark

You can deploy the benchmark with the following command:

```bash
poetry run multi_x_serverless deploy
```

And then run the benchmark with the following command:

```bash
poetry run multi_x_serverless run markdown2html-version_number -a '{"markdown": "IyBIZWxsbywgV29ybGQhClRoaXMgaXMgYSB0ZXN0IG9mIHRoZSBNYXJrZG93biB0byBIVE1MIGNvbnZlcnNpb24u"}'
```

To remove the benchmark, you can use the following command:

```bash
poetry run multi_x_serverless remove markdown2html-version_number
```
