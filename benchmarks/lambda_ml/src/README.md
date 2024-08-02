
# $\lambda$-ML

$\lambda$-ML is a machine learning system built in serverless infrastructure (Amazon AWS Lambda).
Serverless compute service lets you run code without provisioning or managing servers, creating workload-aware cluster scaling logic, maintaining event integrations, or managing runtimes. 
Different from VM-based cloud compute, compute instances in serverless infrastructure cannot communicate with each other.
To solve this problem, $\lambda$-ML implements various communication patterns using external storage.


## Video Tutorials

We provide several video tutorials on YouTube.

- [Introduction to $\lambda$-ML](https://youtu.be/yUzzdp4IY7k)
- [Programming Interface](https://youtu.be/YU0974fViSU)
- [Deploying $\lambda$-ML with S3](https://youtu.be/E_kzXTm32EM)
- [Deploying $\lambda$-ML with ElastiCache](https://youtu.be/58PMo2N8rxA)
- [Deploying $\lambda$-ML with DynamoDB](https://youtu.be/mWa3NpCcEDU)
- [Deploying $\lambda$-ML with Hybrid Parameter Server](https://youtu.be/gjmEV0RCaak)


## Dependencies
- awscli (version 1)
- botocore
- boto3
- numpy
- torch=1.0.1
- thrift
- redis
- grpcio

## Environment setup

- Create a Lambda layer with PyTorch 1.0.1.
- Compress the whole project and upload to Lambda.
- Create a VPC and a security group in AWS.

## Programming Interface

$\lambda$-ML leverages external storage services, e.g., S3, Elasticache, and DynamoDB, to implement communication between serverless compute instances.
We provide both storage interfaces and communication primitives.

### Storage

The storage layer offers basic operations to manipulate external storage.

- S3 ([storage/s3/s3_type.py](storage/s3/s3_type.py)). storage operations: list/save/load/delete/clear/...
- Elasticache ([storage/memcached/memcached_type.py](storage/memcached/memcached_type.py)). storage operations: list/save/load/delete/clear/...
- DynamoDB ([storage/dynamo/dynamo_type.py](storage/dynamo/dynamo_type.py)). storage operations: list/save/load/delete/clear/...

### Communication primitive

The communication layer provides popular communication primitives.

- S3 communicator ([communicator/s3_comm.py](communicator/s3_comm.py)). primitives: async/reduce/reduce_scatter.
- Elasticache communicator ([communicator/memcached_comm.py](communicator/memcached_comm.py)). primitives: async/reduce/reduce_scatter.
- DynamoDB communicator ([communicator/dynamo_comm.py](communicator/dynamo_comm.py)). primitives: async/reduce/reduce_scatter.

### Hybrid framework.

In addition to storage services, $\lambda$-ML also implements a hybrid architecture ---
one VM acts as a parameter server and serverless instances communicate with the VM.
- Launch parameter server. see [thrift_ps/start_service.py](thrift_ps/start_service.py)
- Communication interfaces: ping/register/pull/push/delete.

## Usage

The general usage of $\lambda$-ML:
1. Partition the dataset and upload to S3.
2. Create a trigger Lambda function and an execution Lambda function.
3. Set configurations (e.g., dataset location) and hyperparameters (e.g., learning rate).
4. Set VPC and security group.
5. Execute the trigger function.
6. See the logs in CloudWatch.

See [examples](reproducibility.md) for more details.


## Contact

If you have any question or suggestion, feel free to contact jiawei.jiang@inf.ethz.ch and ce.zhang@inf.ethz.ch.


## Reference
Jiawei Jiang, Shaoduo Gan, Yue Liu, Fanlin Wang, Gustavo Alonso, Ana Klimovic, Ankit Singla, Wentao Wu, Ce Zhang.
[Towards Demystifying Serverless Machine Learning Training](https://arxiv.org/abs/2105.07806). *SIGMOD* 2021.
