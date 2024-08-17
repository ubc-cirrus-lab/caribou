#!/bin/bash
# 10 thread on one machine
# ./rcv_lr_grad_avg_local.sh 10 100 1000 0.01 47236 127.0.0.1:24000 ~/dataset/rcv-1/
# one thread on one machine
# nohup python -u rcv_lr_grad_avg.py --init-method tcp://127.0.0.1:24000 --rank 0 --world-size 1 --epochs 100 -lr 0.01 --batch-size 10000 --features 47236 --root ~/dataset/rcv-1/ --no-cuda > rcv1_lr_grad_avg_w1_lr.01_b10k.log 2>&1 &

world_size=$1
epochs=$2
batch_size=$3
lr=$4
features=$5
master_ip=$6
root=$7

for ((i=0; i<$world_size; i++)); do
  source /home/ubuntu/envs/pytorch/bin/activate
  cd /home/ubuntu/code/lambda/ec2/lr
  log_file=$i"_"$world_size".log"
  rm -f ~/logs/$log_file
  nohup python -u rcv_lr_grad_avg.py --init-method "tcp://${master_ip}" --rank $i --world-size $world_size --epochs $epochs --batch-size $batch_size -lr $lr --features $features --root root --no-cuda > ~/logs/$log_file 2>&1 &
done
