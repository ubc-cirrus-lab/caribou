# Benchmark Inputs and Sources
The following are the sources or instructions to acquire similar input data to reproduce the results obtained in the Caribou paper. 
Due to potential copyright concerns, we do not directly upload the input data used in the publication.

## DNA Visualization (Paper benchmark)
Associated S3 bucket name: `caribou-dna-visualization`

Similar input data: <https://www.ncbi.nlm.nih.gov/genbank/>

Small Input (69KB) source link: <https://www.ncbi.nlm.nih.gov/nuccore/AE001578.1>

Large Input (1.1MB) (almost exact) source link: <https://www.ncbi.nlm.nih.gov/nuccore/AE017199.1>

Use the `Send to` option, select `Complete Record`, and choose `File` to download the data from each of the links above. 
Then, upload this data into the `genbank` folder of the S3 bucket and follow the instructions in the benchmark's individual `readme.md` file.

Note: For the large input, we could not locate the exact version of the nucleotide, as we used a slightly older variation that is no longer publicly available (or we cannot otherwise locate it).

Additional params: None

## RAG Data Ingestion (Paper benchmark)
Associated S3 bucket name: `caribou-document-embedding-benchmark`

Similar input data: Most PDF documents are acceptable inputs.

Small Input (33 pages) source link: <https://www.uscis.gov/sites/default/files/document/guides/M-654.pdf>

Large Input (115 pages) source link: <https://laws-lois.justice.gc.ca/pdf/const_trd.pdf>

Place the respective PDF file in the `input` folder of the S3 bucket.

Note: This benchmark requires many other dependencies to run correctly. Please follow all instructions in the benchmark's individual `readme.md` file.

Additional params: `"user_id": "auto_gen_random_id"` ***Make sure to use a different user_ID for every run if you are running this for benchmark purposes!!!***

## Image Processing (Paper benchmark)
Associated S3 bucket name: `caribou-image-processing-benchmark`

Similar input data: Most image files are acceptable inputs.

Large Input (2.4MB) source link: <https://imgur.com/a/DmiKtIg>

Small Input (222KB): This is a downsized version of the large input, resized to 1210 x 908 pixels and approximately 1/10th of the original file size.

Place the respective image file in the `input` folder of the S3 bucket.

Note: Image processing is a very transmission-heavy application, which is unsuitable for geospatial migration at the current time. 
As a result, most image files will produce similar results to those presented in Fig. 7 of the Caribou paper.

Additional params: `"desired_transformations": ["flip", "rotate", "blur", "greyscale", "resize"]`

## Text2Speech Censoring (Paper benchmark)
Associated S3 bucket name: `caribou-text-2-speech-censoring`

Similar input data: Use the `yelp_academic_dataset_review.json` from <https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset>

Small Input (12 KB, 239 words, 1,301 characters): Use review(s) from the `yelp_academic_dataset_review.json` dataset with similar word or character counts.

Large Input (1 KB, 2,119 words, 11,769 characters): Use review(s) from the `yelp_academic_dataset_review.json` dataset with similar word or character counts.

Place the respective text input file in the `input` folder of the S3 bucket.

Additional Params: None

## Video Analytics (Paper benchmark)
Associated S3 bucket name: `caribou-video-analytics`

Similar input data: Any video file from <https://www.ino.ca/en/technologies/video-analytics-dataset/> converted to mp4.

Small Input (206KB): Use the `INO Parking evening` video from <https://www.ino.ca/en/technologies/video-analytics-dataset/videos/>. Download the RGB version of the video and convert it to mp4.

Large Input (2.4MB): Use the `INO Group fight` video from <https://www.ino.ca/en/technologies/video-analytics-dataset/videos/>. Download the RGB version of the video and convert it to mp4.

Place the respective video input file in the `input` folder of the S3 bucket.

Note: This benchmark also requires the `imagenet_labels.txt` file from <https://github.com/vhive-serverless/vSwarm/blob/main/benchmarks/video-analytics/object_recognition/imagenet_labels.txt> which must be present in the S3 bucket.

Additional note: Please read and follow the terms of the LICENSE in each of the video files.

Additional params: None