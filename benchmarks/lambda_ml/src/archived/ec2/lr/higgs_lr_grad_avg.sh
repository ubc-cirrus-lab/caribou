#!/bin/bash

world_size=$1

batch_size=$2
# batch_size is the global batch size
#batch_size_global=`expr $world_size \* $batch_size_local`

log_file=$3

for ((i=0; i<$world_size; i++)); do
  	if [ $i == 0 ]
	then
	  source /home/ubuntu/envs/pytorch/bin/activate
	  cd /home/ubuntu/code/LambdaML/ec2
	  rm -f $log_file
	  nohup python higgs_lr_grad_avg.py --init-method tcp://172.31.44.193:24000 --rank 0 --world-size $world_size --root /home/ubuntu/data/s3/ --no-cuda --batch-size $batch_size > $log_file 2>&1 &
	else
      ssh lambdacluster-node00$i "source /home/ubuntu/envs/pytorch/bin/activate; cd /home/ubuntu/code/LambdaML/ec2; rm -f $log_file; nohup python higgs_lr_avg_grad.py --init-method tcp://172.31.44.193:24000 --rank $i --world-size $world_size --root /home/ubuntu/data/higgs/ --no-cuda --batch-size $batch_size > $log_file 2>&1 &"	
  	fi
done
