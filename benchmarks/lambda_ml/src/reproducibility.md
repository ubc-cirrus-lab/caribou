# Reproducibility

This document shows the workloads in the LambdaML paper [1] and describes how to run these workloads.

## 1. S3

We use S3 storage as external storage to implement communication in serverless infrastructure.


### Linear models

- Trigger function.
[linear_s3_trigger.py](examples/lambda/s3/linear_s3_trigger.py). 
    - Set *data_bucket* and *file* according to the specific dataset.
    - Set *dataset_type* to dense_libsvm for dense dataset or sparse_libsvm for sparse dataset.
    - Set *model* to lr/svm for dense model or sparse_lr/sparse_svm for sparse model.
    - Set *optim* to grad_avg for gradient average (SGD), model_avg for model average, or admm for ADMM.
    - Set *sync_mode* to reduce, reduce_scatter or async.
    - Set other configurations and hyperparameters.
- Execution function. Choose the execution function according to sparsity and optimization algorithm.
    - Dense model, SGD or Model Average. [linear_s3_ga_ma.py](examples/lambda/s3/linear_s3_ga_ma.py)
    - Dense model, ADMM. [linear_s3_admm.py](examples/lambda/s3/linear_s3_admm.py)
    - Sparse model, SGD or Model Average. [sparse_linear_s3_ga_ma.py](examples/lambda/s3/sparse_linear_s3_ga_ma.py)
    - Sparse model, ADMM. [sparse_linear_s3_admm.py](examples/lambda/s3/sparse_linear_s3_admm.py)

### KMeans
- Trigger function.
[kmeans_s3_trigger.py](examples/lambda/s3/kmeans_s3_trigger.py). 
    - Set *data_bucket* and *file* according to the specific dataset.
    - Set *dataset_type* to dense_libsvm for dense dataset or sparse_libsvm for sparse dataset.
    - Set *sync_mode* to reduce or reduce_scatter.
    - Set other configurations and hyperparameters.
- Execution function. Choose the execution function according to sparsity.
    - Dense model. [kmeans_s3.py](examples/lambda/s3/kmeans_s3.py)
    - Sparse model. [sparse_kmeans_s3.py](examples/lambda/s3/sparse_kmeans_s3.py)

### CNN models
- Trigger function.
[dl_s3_trigger.py](examples/lambda/s3/dl_s3_trigger.py). 
    - Set *data_bucket*, *train_file* and test_file according to the specific dataset.
    - Set *model* to mobilenet or resnet.
    - Set *optim* to grad_avg for gradient average (SGD) or model_avg for model average.
    - Set *sync_mode* to reduce, reduce_scatter or async.
    - Set *start_epoch* (the starting epoch) and *run_epochs* (number of epochs to run in the current execution). 
    LambdaML will read checkpoint model of *start_epoch* from *cp_bucket*.
    - Set other configurations and hyperparameters.
- Execution function. [dl_s3.py](examples/lambda/s3/dl_s3.py).

## 2. Elasticache

We use Elasticache for Memcached as external storage to implement communication in serverless infrastructure.

### Linear models
- Create an Elasticache cluster in AWS.
- Trigger function.
[linear_ec_trigger.py](examples/lambda/elasticacge/linear_ec_trigger.py). 
    - Set *host* and *port* of the Elasticache cluster.
    - Set *data_bucket* and *file* according to the specific dataset.
    - Set *dataset_type* to dense_libsvm for dense dataset or sparse_libsvm for sparse dataset.
    - Set *model* to lr/svm for dense model or sparse_lr/sparse_svm for sparse model.
    - Set *optim* to grad_avg for gradient average (SGD), model_avg for model average, or admm for ADMM.
    - Set *sync_mode* to reduce, reduce_scatter or async.
    - Set other configurations and hyperparameters.
- Execution function. Choose the execution function according to sparsity and optimization algorithm.
    - Dense model, SGD or Model Average. [linear_ec_ga_ma.py](examples/lambda/elasticache/linear_ec_ga_ma.py)
    - Dense model, ADMM. [linear_ec_admm.py](examples/lambda/elasticache/linear_ec_admm.py)
    - Sparse model, SGD or Model Average. [sparse_linear_ec_ga_ma.py](examples/lambda/elasticache/sparse_linear_ec_ga_ma.py)
    - Sparse model, ADMM. [sparse_linear_ec_admm.py](examples/lambda/elasticache/sparse_linear_ec_admm.py)
- Add the same VPC and security group to the trigger function, the execution, and the memcached cluster.
- If the model size is larger than 1MB, change the value of *max_item_size* in the parameter group of Elasticache.

### KMeans
- Create an Elasticache cluster in AWS.
- Trigger function.
[kmeans_ec_trigger.py](examples/lambda/elasticache/kmeans_ec_trigger.py). 
    - Set *host* and *port* of the Elasticache cluster.
    - Set *data_bucket* and *file* according to the specific dataset.
    - Set *dataset_type* to dense_libsvm for dense dataset or sparse_libsvm for sparse dataset.
    - Set *sync_mode* to reduce or reduce_scatter.
    - Set other configurations and hyperparameters.
- Execution function. Choose the execution function according to sparsity.
    - Dense model. [kmeans_ec.py](examples/lambda/elasticache/kmeans_ec.py)
    - Sparse model. [sparse_kmeans_ec.py](examples/lambda/elasticache/sparse_kmeans_ec.py)
- Set up VPC, security group in the same way as linear models.

### CNN models
- Create an Elasticache cluster in AWS.
- Trigger function.
[dl_ec_trigger.py](examples/lambda/elasticache/kmeans_ec_trigger.py). 
    - Set *host* and *port* of the Elasticache cluster.
    - Set *data_bucket*, *train_file* and *test_file* according to the specific dataset.
    - Set *model* to mobilenet or resnet.
    - Set *optim* to grad_avg for gradient average (SGD) or model_avg for model average.
    - Set *sync_mode* to reduce, reduce_scatter or async.
    - Set *start_epoch* (the starting epoch) and *run_epochs* (number of epochs to run in the current execution). 
    LambdaML will read checkpoint model of *start_epoch* from *cp_bucket*.
    - Set other configurations and hyperparameters.
- Execution function. [dl_ec.py](examples/lambda/elasticache/dl_ec.py).
- Set up VPC, security group in the same way as linear models.

## 3. DynamoDB

We use DynamoDB as external storage to implement communication in serverless infrastructure.
Note that, the maximal item size of DynamoDB is 400KB ([link](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html)).
Therefore, we do not train CNN models and sparse models with DynamoDB.

### Linear models
- Create two DynamoDB tables in AWS.
- Trigger function.
[linear_dynamo_trigger.py](examples/lambda/dynamo/linear_dynamo_trigger.py). 
    - Set two tables used during training --- *tmp_table_name* and *merged_table_name*.
    - Set *data_bucket* and *file* according to the specific dataset.
    - Set *dataset_type* to dense_libsvm.
    - Set *model* to lr or svm.
    - Set *optim* to grad_avg for gradient average (SGD), model_avg for model average, or admm for ADMM.
    - Set *sync_mode* to reduce, reduce_scatter or async.
    - Set other configurations and hyperparameters.
- Execution function. Choose the execution function according to optimization algorithm.
    - SGD or Model Average. [linear_dynamo_ga_ma.py](examples/lambda/dynamo/linear_dynamo_ga_ma.py)
    - ADMM. [linear_dynamo_admm.py](examples/lambda/dynamo/linear_dynamo_admm.py)

### KMeans
- Create two DynamoDB tables in AWS.
- Trigger function.
[kmeans_dynamo_trigger.py](examples/lambda/dynamo/kmeans_dynamo_trigger.py). 
    - Set two tables used during training --- *tmp_table_name* and *merged_table_name*.
    - Set *data_bucket* and *file* according to the specific dataset.
    - Set *dataset_type* to dense_libsvm.
    - Set *sync_mode* to reduce or reduce_scatter.
    - Set other configurations and hyperparameters.
- Execution function. [kmeans_dynamo.py](examples/lambda/dynamo/kmeans_dynamo.py)

## 4. Hybrid

We use a VM as a parameter server and let serverless instances communicate with the parameter server.

### Linear models
- Launch a parameter server using [start_service.py](thrift_ps/start_service.py).
    - Example: python start_service.py --host $host --port $port --interval 60 --expired 6000 --dir $dir
- Trigger function.
[linear_hybrid_trigger.py](examples/lambda/hybrid/linear_hybrid_trigger.py). 
    - Set *host* and *port* of the parameter server.
    - Set *data_bucket* and *file* according to the specific dataset.
    - Set *dataset_type* to dense_libsvm for dense dataset or sparse_libsvm for sparse dataset.
    - Set *model* to lr/svm for dense model or sparse_lr/sparse_svm for sparse model.
    - Set *optim* to grad_avg for gradient average (SGD).
    - Set *sync_mode* to reduce.
    - Set other configurations and hyperparameters.
- Execution function. Choose the execution function according to sparsity and optimization algorithm.
    - Dense model, SGD. [linear_hybrid_ga.py](examples/lambda/hybrid/linear_hybrid_ga.py)
    - Sparse model, SGD. [sparse_linear_hybrid_ga.py](examples/lambda/hybrid/sparse_linear_hybrid_ga.py)
- Add the same VPC and security group to the trigger function, the execution, and the parameter server.

### KMeans
- Launch a parameter server using [start_service.py](thrift_ps/start_service.py).
- Trigger function.
[kmeans_hybrid_trigger.py](examples/lambda/hybrid/kmeans_hybrid_trigger.py). 
    - Set *host* and *port* of the parameter server.
    - Set *data_bucket* and *file* according to the specific dataset.
    - Set *dataset_type* to dense_libsvm for dense dataset or sparse_libsvm for sparse dataset.
    - Set *sync_mode* to reduce.
    - Set other configurations and hyperparameters.
- Execution function. Choose the execution function according to sparsity.
    - Dense model. [kmeans_hybrid.py](examples/lambda/hybrid/kmeans_hybrid.py)
    - Sparse model. [sparse_kmeans_hybrid.py](examples/lambda/hybrid/sparse_kmeans_hybrid.py)
- Set up VPC, security group in the same way as linear models.

### CNN models
- Launch a parameter server using [start_service.py](thrift_ps/start_service.py).
- Trigger function.
[dl_hybrid_trigger.py](examples/lambda/hybrid/kmeans_hybrid_trigger.py). 
    - Set *host* and *port* of the parameter server.
    - Set *data_bucket*, *train_file* and *test_file* according to the specific dataset.
    - Set *model* to mobilenet or resnet.
    - Set *optim* to grad_avg for gradient average (SGD) or model_avg for model average.
    - Set *sync_mode* to reduce.
    - Set *start_epoch* (the starting epoch) and *run_epochs* (number of epochs to run in the current execution). 
    LambdaML will read checkpoint model of *start_epoch* from *cp_bucket*.
    - Set other configurations and hyperparameters.
- Execution function. [dl_ec.py](examples/lambda/elasticache/dl_ec.py).
- Set up VPC, security group in the same way as linear models.


## 5. Other workloads

### Runtime larger than 15 minutes.

AWS Lambda restricts that the maximal runtime of an execution is 15 minutes.
If a training task runs more than 15 minutes, LambdaML stores a checkpoint model to S3 and
invokes another round of the execution function with the checkpoint model.
Refer to [dl_s3.py](examples/lambda/s3/dl_s3.py) for an example for CNN model.
The scripts for other models can be easily written in the same way.
The following shows the necessary setups.

- Set *run_epochs* (the number of epochs to run in the current execution).
- Set *cp_bucket* (the S3 bucket to save checkpoint models).
- If your execution Lambda function is configured with a VPC, make sure the function can be self-invoked.
For example, you can create a VPC endpoint (refer to [link](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc-endpoints.html)).


## Reference
Jiawei Jiang, Shaoduo Gan, Yue Liu, Fanlin Wang, Gustavo Alonso, Ana Klimovic, Ankit Singla, Wentao Wu, Ce Zhang.
Towards Demystifying Serverless Machine Learning Training. SIGMOD 2021 (to appear).