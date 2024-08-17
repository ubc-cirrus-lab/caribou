#!/bin/bash
# 16 thread on one machine
# ./run_higgs_svm_local.sh 16 6250 0.01 100 172.31.41.172:24000
# one thread on one machine
# nohup python -u higgs_svm_grad_avg.py --init-method tcp://127.0.0.1:24000 --world-size 1 --rank 0 --root /home/ubuntu/dataset/s3 --no-cuda --batch-size 100000 --learning-rate 0.01 --epochs 10 > logs/0_1.log 2>&1 &

world_size=$1

batch_size=$2

learning_rate=$3

epochs=$4

master_ip=$5

for ((i=0; i<$world_size; i++)); do
  source /home/ubuntu/envs/pytorch/bin/activate
  #cd /home/ubuntu/code/lambda/ec2
  log_file=$i"_"$world_size".log"
  rm -f logs/$log_file
  nohup python higgs_svm_grad_avg.py --init-method tcp://$master_ip --world-size $world_size --rank $i --root /home/ubuntu/dataset/s3/ --no-cuda --batch-size $batch_size --learning-rate $learning_rate --epochs $epochs > logs/$log_file 2>&1 &
done
