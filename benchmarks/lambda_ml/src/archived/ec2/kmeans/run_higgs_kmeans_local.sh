#!/bin/bash
# 1 thread on one machine
# ./run_higgs_kmeans_local.sh 1 100 10 30 127.0.0.1:26000 ~/dataset/s3-1/
# one thread on one machine
# nohup python -u higgs_kmeans.py --init-method tcp://127.0.0.1:26000 --rank 0 --world-size 1 --epochs 100 -k 10 --features 30 --root ~/dataset/s3-1/ --no-cuda > higgs_kmeans_w1_r0_k10.log 2>&1 &

world_size=$1
epochs=$2
n_cluster=$3
n_features=$4
master_ip=$5
root=$6

for ((i=0; i<$world_size; i++)); do
  source /home/ubuntu/envs/pytorch/bin/activate
  cd /home/ubuntu/code/lambda/ec2/kmeans
  file_path="${root}/${i}_${world_size}"
  dir_name="higgs_kmeans_w${world_size}_k${n_cluster}"
  if [ -d ~/logs/$dir_name ]; then rm -rf ~/logs/$dir_name; fi
  mkdir ~/logs/$dir_name
  log_name=$i"_"$world_size".log"
  rm -f ~/logs/$dir_name/$log_name
  nohup python -u higgs_kmeans.py --init-method tcp://${master_ip} --rank $i --world-size $world_size --communication all-reduce --epochs $epochs -k $n_cluster --features $n_features --root $root --train-file $file_path --no-cuda > ~/logs/$dir_name/$log_name 2>&1 &
done
