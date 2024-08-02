#!/bin/bash
# 16 thread on one machine
# ./higgs_lr_grad_avg_local.sh 16 100 6250 0.01 30 172.31.41.172:24000 ~/dataset/s3/
# one thread on one machine
# nohup python -u higgs_lr_grad_avg.py --init-method tcp://127.0.0.1:24000 --world-size 1 --rank 0 --root /home/ubuntu/dataset/s3 --no-cuda --batch-size 100000 --learning-rate 0.01 --epochs 10 > logs/0_1.log 2>&1 &

world_size=$1
epochs=$2
batch_size=$3
learning_rate=$4
features=$5
master_ip=$6
root=$7

for ((i=0; i<$world_size; i++)); do
  source /home/ubuntu/envs/pytorch/bin/activate
  cd /home/ubuntu/code/lambda/ec2/lr
  log_file=$i"_"$world_size".log"
  rm -f /home/ubuntu/logs/$log_file
  nohup python -u higgs_lr_grad_avg.py --init-method tcp://$master_ip --rank $i --world-size $world_size --epochs $epochs --batch-size $batch_size --learning-rate $learning_rate --features $features --root $root --no-cuda > /home/ubuntu/logs/$log_file 2>&1 &
done
