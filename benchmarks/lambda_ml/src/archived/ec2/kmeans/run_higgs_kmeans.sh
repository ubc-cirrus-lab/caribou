#!/bin/bash

world_size=$1
nr_cluster=$2
master_ip=$3

train_path="/home/ubuntu/dataset/higgs${world_size}"

for ((i=0; i<world_size; i++)); do
        if [ $i == 0 ]
        then
                source /home/ubuntu/envs/pytorch/bin/activate
                nohup python3.6 /home/ubuntu/LambdaML/ec2/kmeans/higgs_kmeans.py --init-method "tcp://${master_ip}" --rank 0 --communication all-reduce -k "${nr_cluster}" --world-size "${world_size}" --train-file "${train_path}/${i}_${world_size}" --no-cuda > "/home/ubuntu/log/distrb_higgs_kmeans_r${i}_w${world_size}_k${nr_cluster}.txt" 2>&1 &
        else
                ssh "higgs-node00${i}" "source /home/ubuntu/envs/pytorch/bin/activate; cd /home/ubuntu/LambdaML/ec2/kmeans; nohup python3.6 higgs_kmeans.py --init-method tcp://${master_ip} --communication all-reduce --rank ${i} -k ${nr_cluster} --world-size ${world_size} --train-file ""${train_path}"/${i}_"${world_size}"" --no-cuda > /home/ubuntu/log/distrib_higgs_kmeans_r${i}_w${world_size}_k${nr_cluster}.txt 2>&1 &"
        fi
done
