#!/bin/bash
# ./run_cifar_mobilenet_local.sh 16 6250 0.01 100 172.31.41.172:24000

number_of_machines=$1

cores_per_machine=$2

batch_size_local=$3

learning_rate=$4

epochs=$5

master_ip=$6

cluster_name=$7

world_size=`expr $number_of_machines \* $cores_per_machine`

batch_size_global=`expr $world_size \* $batch_size_local`

# echo $world_size
# echo $batch_size_global

# for ((i=0; i<$world_size; i++)); do
#         if [ $i -le 9 ]; then
#             echo $cluster_name-node00$i &
#         else
#         	echo $cluster_name-node0$i &
#             echo `expr $i \* $cores_per_machine + $k`
#         fi
# done


# for ((i=0; i<$world_size; i++)); do
#   source /home/ubuntu/envs/pytorch/bin/activate
#   log_file=$i"_"$world_size".log"
#   rm -f logs/$log_file
#   nohup python cifar10_mobilenet_grad_avg.py --init-method tcp://$master_ip --world-size $world_size --rank $i --root /home/ubuntu/dataset/cifar10/ --no-cuda --batch-size $batch_size --learning-rate $learning_rate --epochs $epochs > logs/$log_file 2>&1 &
# done

# cluster-128-node004

for ((i=0; i<$number_of_machines; i++)); do

        if [ $i -eq 0 ]; then
            source /home/ubuntu/envs/pytorch/bin/activate
            for ((j=0; j<$cores_per_machine; j++)); do    
                python3.6 /home/ubuntu/LambdaML/ec2/cnn/cifar10_mobilenet_grad_avg.py --init-method tcp://$master_ip --rank $j --world-size $world_size --root /mnt --no-cuda --batch-size $batch_size_global --learning-rate $learning_rate --epochs $epochs &
            done
           
        elif [ $i -le 9 ]; then
            for ((k=0; k<$cores_per_machine; k++)); do

                ssh $cluster_name-node00$i "source /home/ubuntu/envs/pytorch/bin/activate; cd /home/ubuntu/LambdaML/ec2/cnn/; python3.6 /home/ubuntu/LambdaML/ec2/cnn/cifar10_mobilenet_grad_avg.py --init-method tcp://$master_ip --rank `expr $i \* $cores_per_machine + $k` --world-size $world_size --root /mnt --no-cuda --batch-size $batch_size_global" &
            done

        else
            for ((p=0; p<$cores_per_machine; p++)); do

                ssh $cluster_name-node0$i "source /home/ubuntu/envs/pytorch/bin/activate; cd /home/ubuntu/LambdaML/ec2/cnn/; python3.6 /home/ubuntu/LambdaML/ec2/cnn/cifar10_mobilenet_grad_avg.py --init-method tcp://$master_ip --rank `expr $i \* $cores_per_machine + $p` --world-size $world_size --root /mnt --no-cuda --batch-size $batch_size_global" &
            done
        fi
done
