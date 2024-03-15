#!/bin/bash

# Parameters
function_name=$1
version_number=$2
num_invocations=$3
timeframe=$4

# Calculate the sleep time between each invocation
sleep_time=$(echo "scale=2; $timeframe / $num_invocations" | bc)

# Invoke the function the specified number of times
for ((i=1; i<=$num_invocations; i++))
do
    poetry run multi_x_serverless run $function_name-$version_number -a '{"gen_file_name": "small_sequence.gb"}'
    sleep $sleep_time
done