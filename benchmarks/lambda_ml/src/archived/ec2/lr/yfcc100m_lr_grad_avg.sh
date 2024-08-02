#!/bin/bash
# 10 machines
# ./yfcc100m_lr_grad_avg.sh 10 20 4096 2 0.01 800 /bigdata/yfcc100m/ 4 animal 172.31.45.55:24000 t2.medium-10

world_size=$1
epochs=$2
n_features=$3
n_classes=$4
lr=$5
batch_size=$6
root=$7
n_files=$8
pos_tag=$9
master_ip=${10}
cluster_name=${11}

home_path="/bigdata/"
env_path="/home/ubuntu/envs/pytorch/bin/activate"
code_path="${home_path}/code/lambda/ec2/lr"

for ((i=0; i<$world_size; i++)); do
  if [[ $i == 0 ]]
	then
	  source $env_path
	  cd $code_path
	  dir_path="${home_path}/logs/LR_yfcc_w${world_size}_f${n_files}_lr${lr}_b${batch_size}/"
	  rm -rf $dir_path
	  mkdir -p $dir_path
    log_path=$dir_path$i"_"$world_size".log"
    rm -f $log_path
	  nohup python -u yfcc100m_lr_grad_avg.py --init-method tcp://$master_ip --rank 0 --world-size $world_size --epochs $epochs \
	  --features $n_features --classes $n_classes --learning-rate $lr --batch-size $batch_size --root $root --num-files $n_files \
	  --pos-tag $pos_tag --no-cuda > $log_path 2>&1 &
	else
	  node_name=""
	  if [[ $i -lt 10 ]]
	  then
	    node_name=${cluster_name}-node00$i
	  elif [[ $i -lt 100 ]]
	  then
	    node_name=${cluster_name}-node0$i
	  else
	    node_name=${cluster_name}-node$i
	  fi
	  dir_path="${home_path}/logs/LR_yfcc_w${world_size}_f${n_files}_lr${lr}_b${batch_size}/"
	  log_path=$dir_path$i"_"$world_size".log"
	  # master and slaves share the same volume, do not need to rm and mkdir.
    ssh $node_name "source $env_path; cd $code_path; nohup python -u yfcc100m_lr_grad_avg.py --init-method tcp://$master_ip --rank $i --world-size $world_size --epochs $epochs --features $n_features --classes $n_classes --learning-rate $lr --batch-size $batch_size --root $root --num-files $n_files --pos-tag $pos_tag --no-cuda > $log_path 2>&1 &"
  fi
done