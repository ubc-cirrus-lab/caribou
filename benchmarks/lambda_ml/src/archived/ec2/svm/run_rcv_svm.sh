#!/bin/bash

world_size=$1
lr=$2
batch_size=$3
master_ip=$4
cluster_name=$5

for ((i=0; i<world_size; i++)); do
        if [ $i == 0 ]
        then
                source /home/ubuntu/envs/pytorch/bin/activate
                nohup python3.6 /home/ubuntu/LambdaML/ec2/svm/rcv_svm.py --init-method "tcp://${master_ip}" --rank 0 --batch-size $batch_size -l $lr --world-size $world_size --train-file /home/ubuntu/dataset/rcv --no-cuda > "/home/ubuntu/log/svm_${i}_${world_size}_${batch_size}_${lr}.txt" 2>&1 &
        else
                ssh "${cluster_name}-node00${i}" "source /home/ubuntu/envs/pytorch/bin/activate; cd /home/ubuntu/LambdaML/ec2/svm/; nohup python3.6 rcv_svm.py --init-method tcp://172.31.42.34:24000 --rank $i --batch-size $batch_size -l $lr --world-size $world_size --train-file /home/ubuntu/dataset/rcv --no-cuda > /home/ubuntu/log/svm_${i}_${world_size}_${batch_size}_${lr}.txt 2>&1 &"
        fi
done

