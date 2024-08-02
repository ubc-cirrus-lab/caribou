#!/bin/bash
# 10 thread on one machine
# ./run_rcv_svm_local.sh 10 100 1000 0.01 47236 127.0.0.1:24000 ~/dataset/rcv-1/
# one thread on one machine
# nohup python -u rcv_svm_grad_avg.py --init-method tcp://127.0.0.1:24000 --rank 0 --world-size 1 --epochs 100 -lr 0.01 --batch-size 10000 --features 47236 --root ~/dataset/rcv-1/ --no-cuda > rcv1_svm_grad_avg_w1_lr0.01_b10k.log 2>&1 &

world_size=$1
epochs=$2
batch_size=$3
lr=$4
features=$5
master_ip=$6
root=$7

for ((i=0; i<$world_size; i++)); do
  source /home/ubuntu/envs/pytorch/bin/activate
  cd /home/ubuntu/code/lambda/ec2/svm
  file_path="${root}/${i}_${world_size}"
  dir_name="rcv1_svm_grad_avg_w${world_size}_lr${lr}_b${batch_size}"
  rm -r ~/logs/$dir_name & mkdir ~/logs/$dir_name
  log_name=$i"_"$world_size".log"
  rm -f ~/logs/$dir_name/$log_name
  nohup python -u rcv_svm_grad_avg.py --init-method tcp://${master_ip} --rank $i --world-size $world_size --epochs $epochs --batch-size $batch_size -lr $lr --features $features --root ~/dataset/rcv-1/ --no-cuda > ~/logs/$dir_name/$log_name 2>&1 &
done
