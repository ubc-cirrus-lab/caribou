#!/bin/bash
# ./run_cifar_mobilenet_local.sh 16 128 0.01 30 127.0.0.1:24000
# nohup python cifar10_mobilenet_grad_avg.py --init-method tcp://172.31.41.172:24000 --world-size 2 --rank 0 --root /home/ubuntu/dataset/cifar10/ --no-cuda --batch-size 128 --learning-rate 0.01 --epochs 10 > logs/tmp.log 2>&1 &

world_size=$1

batch_size=$2

learning_rate=$3

epochs=$4

master_ip=$5

for ((i=1; i<$world_size; i++)); do
  source /home/ubuntu/envs/pytorch/bin/activate
  log_file=$i"_"$world_size".log"
  rm -f logs/$log_file
  nohup python -u cifar10_mobilenet_grad_avg.py --init-method tcp://$master_ip --world-size $world_size --rank $i --root /home/ubuntu/dataset/cifar10/ --no-cuda --batch-size $batch_size --learning-rate $learning_rate --epochs $epochs > logs/$log_file 2>&1 &
done

python -u cifar10_mobilenet_grad_avg.py --init-method tcp://$master_ip --world-size $world_size --rank 0 --root /home/ubuntu/dataset/cifar10/ --no-cuda --batch-size $batch_size --learning-rate $learning_rate --epochs $epochs > logs/master.log

nohup python -u cifar10_resnet50_grad_avg.py --init-method tcp://127.0.0.1:24000 --world-size 1 --rank 0 --root /mnt/cifar-10-batches-py --batch-size 320 --learning-rate 0.01 --epochs 100 > logs/master.log 2>&1 &


# if more than 10 nodes
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
