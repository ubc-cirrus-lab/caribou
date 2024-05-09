#!/bin/bash

N_JOBS=5;
T_SLEEP="10";

echo "Running Job...";
for ((i = 0; i < N_JOBS; i++))
do
  caribou run map_reduce-0.0.1 -a '{"input_base_dir": "mr-input", "number_shards": 8, "input_file_size": 3307252}';
done
echo "Sleeping for ${T_SLEEP} seconds";
sleep ${T_SLEEP};
echo "Syncing logs";
caribou log_sync;
echo "Collecting data";
caribou data_collect all --workflow_id map_reduce-0.0.1;
echo "Running manage deployments";
caribou manage_deployments;
