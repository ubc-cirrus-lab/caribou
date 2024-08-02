#!/bin/bash
# 10 workers, data size = 10
# ./run_comm_test.sh 10 10 28 172.31.36.200:26000 sparse
# ./run_comm_test.sh 2 10 100000000 172.31.41.254:26000 t2.medium-10


world_size=$1
epochs=$2
data_size=$3
master_ip=$4
cluster_name=$5

home_path="/bigdata/"
env_path="/home/ubuntu/envs/pytorch/bin/activate"
code_path="${home_path}/code/lambda/ec2/test"

for ((i=0; i<$world_size; i++)); do
  dir_path="${home_path}/logs/serverless/comm_test_w${world_size}_size${data_size}/"
  log_path=$dir_path$i"_"$world_size".log"
  if [[ $i == 0 ]]
	then
	  source $env_path
	  cd $code_path
	  rm -rf $dir_path
	  mkdir -p $dir_path
    rm -f $log_path
	  nohup python -u comm_test.py --init-method tcp://$master_ip --rank 0 --world-size $world_size --epochs $epochs --data-size $data_size --no-cuda > $log_path 2>&1 &
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
	  # master and slaves share the same volume, do not need to rm and mkdir.
    ssh $node_name "source $env_path; cd $code_path; nohup python -u comm_test.py --init-method tcp://$master_ip --rank $i --world-size $world_size --epochs $epochs --data-size $data_size --no-cuda > $log_path 2>&1 &"
  fi
done
