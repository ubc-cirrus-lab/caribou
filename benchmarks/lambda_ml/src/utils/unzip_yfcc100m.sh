#!/bin/bash
# ./unzip_yfcc100m.sh /mnt/ds3lab-scratch/jiawei/yfcc100m/original_data/ 4 96

dir_path=$1
start_index=$2
end_index=$3

cd ${dir_path}

for ((i=${start_index}; i<=${end_index}; i++)); do
  data_path="${dir_path}/YFCC100M_hybridCNN_gmean_fc6_${i}.txt.gz"
  gzip -d ${data_path}
done