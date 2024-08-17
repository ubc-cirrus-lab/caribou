#!/bin/bash
# 10 machines
# ./higgs_lr_random_admm_fl.sh 10 50 10 30 2 0.01 10000 /bigdata/s3/ 172.31.35.34:24000 t2.medium-10

world_size=$1
admm_epochs=$2
epochs=$3
n_features=$4
n_classes=$5
lr=$6
batch_size=$7
root=$8
master_ip=${9}
cluster_name=${10}

home_path="/bigdata/"
env_path="/home/ubuntu/envs/pytorch/bin/activate"
code_path="${home_path}/code/lambda/ec2/lr"

for ((i=0; i<$world_size; i++)); do
  if [[ $i == 0 ]]
	then
	  source $env_path
	  cd $code_path
	  dir_path="${home_path}/logs/LR_higgs_admm_fl_w${world_size}_lr${lr}_b${batch_size}/"
	  rm -rf $dir_path
	  mkdir -p $dir_path
    log_path=$dir_path$i"_"$world_size".log"
    rm -f $log_path
	  nohup python -u higgs_lr_random_admm_fl.py --init-method tcp://$master_ip --rank 0 --world-size $world_size \
	  --admm-epochs $admm_epochs --epochs $epochs --features $n_features --classes $n_classes \
	  --learning-rate $lr --batch-size $batch_size --root $root --no-cuda > $log_path 2>&1 &
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
	  dir_path="${home_path}/logs/LR_higgs_admm_fl_w${world_size}_lr${lr}_b${batch_size}/"
	  log_path=$dir_path$i"_"$world_size".log"
	  # master and slaves share the same volume, do not need to rm and mkdir.
    ssh $node_name "source $env_path; cd $code_path; nohup python -u higgs_lr_random_admm_fl.py --init-method tcp://$master_ip --rank $i --world-size $world_size --admm-epochs $admm_epochs --epochs $epochs --features $n_features --classes $n_classes --learning-rate $lr --batch-size $batch_size --root $root --no-cuda > $log_path 2>&1 &"
  fi
done