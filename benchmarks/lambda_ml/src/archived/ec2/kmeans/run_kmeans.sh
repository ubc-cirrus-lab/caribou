#!/bin/bash

# bash run_kmeans.sh [world_size] [nr_cluster] [master_ip] [cluster_name]
world_size=$1
nr_cluster=$2
master_ip=$3
cluster_name=$4

train_path="/home/ubuntu/dataset/rcv${world_size}"

for ((i=0; i<world_size; i++)); do
        if [ $i == 0 ]
        then
                source /home/ubuntu/envs/pytorch/bin/activate
                nohup python3.6 /home/ubuntu/LambdaML/ec2/kmeans/rcv_kmeans.py --init-method "tcp://${master_ip}" --rank 0 --communication all-reduce -k "${nr_cluster}" --world-size "${world_size}" --train-file "${train_path}/${i}_${world_size}" --no-cuda > "/home/ubuntu/log/kmeans_${i}_${world_size}_${nr_cluster}.txt" 2>&1 &
        else
                ssh "${cluster_name}-node00${i}" "source /home/ubuntu/envs/pytorch/bin/activate; cd /home/ubuntu/LambdaML/ec2/kmeans; nohup python3.6 rcv_kmeans.py --init-method tcp://${master_ip} --communication all-reduce --rank ${i} -k ${nr_cluster} --world-size ${world_size} --train-file ""${train_path}"/${i}_"${world_size}"" --no-cuda > /home/ubuntu/log/kmeans_${i}_${world_size}_${nr_cluster}.txt 2>&1 &"
        fi
done
