#!/bin/bash

world_size=$1

number_of_machines=$2
cores_per_machine=$3
master_ip=$4


# one machine
for ((i=0; i<$world_size; i++)); do
  /home/ubuntu/anaconda3/envs/pytorch_p36/bin/python3 -u /home/ubuntu/LambdaML/ec2/cnn/cifar10_resnet50_grad_avg.py --rank $i --world-size $world_size --root /home/ubuntu/cifar10 --no-cuda &
done

# multiple machines
# for ((i=0; i<$number_of_machines; i++)); do

#         if [ $i -eq 0 ]; then
#             for ((j=0; j<$cores_per_machine; j++)); do   
#                 /home/ubuntu/anaconda3/envs/pytorch_p36/bin/python3 -u /home/ubuntu/LambdaML/ec2/cnn/cifar10_resnet50_grad_avg.py --init-method tcp://$master_ip --rank $j --world-size $world_size --root /home/ubuntu --no-cuda &
#             done
#         else
#             for ((p=0; p<$cores_per_machine; p++)); do
#                 ssh resnet-10-node00$i "/home/ubuntu/anaconda3/envs/pytorch_p36/bin/python3 -u /home/ubuntu/LambdaML/ec2/cnn/cifar10_resnet50_grad_avg.py --init-method tcp://$master_ip --rank `expr $i \* $cores_per_machine + $p` --world-size $world_size --root /home/ubuntu --no-cuda" &
#             done
#         fi
# done


# for ((i=0; i<$world_size; i++)); do
#         if [ $i -eq 0 ]; then
#             source /home/ubuntu/envs/pytorch/bin/activate
#             python3.6 /home/ubuntu/code_2.0/LambdaML/ec2/cifar10_resnet50.py --init-method tcp://172.31.44.193:24000 --rank 0 --world-size $world_size --root /mnt --no-cuda --batch-size $batch_size_global &
#         elif [ $i -le 9 ]; then
#             ssh cluster20-node00$i "source /home/ubuntu/envs/pytorch/bin/activate; cd /home/ubuntu/code_2.0/LambdaML/ec2/; python3.6 /home/ubuntu/code_2.0/LambdaML/ec2/cifar10_resnet50.py --init-method tcp://172.31.44.193:24000 --rank $i --world-size $world_size --root /mnt --no-cuda --batch-size $batch_size_global" &
#         else
#         	ssh cluster20-node0$i "source /home/ubuntu/envs/pytorch/bin/activate; cd /home/ubuntu/code_2.0/LambdaML/ec2/; python3.6 /home/ubuntu/code_2.0/LambdaML/ec2/cifar10_resnet50.py --init-method tcp://172.31.44.193:24000 --rank $i --world-size $world_size --root /mnt --no-cuda --batch-size $batch_size_global" &
#         fi
# done
