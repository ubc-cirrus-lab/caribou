# Global Latency Tracker

This is an initial version of a latency tracker that can be rolled out globally (on AWS) for now and which simply tracks the latency of a request from one lambda region to another.

For this to work, you need to deploy the latency tracker in all regions that you want to track the latency for. The latency tracker will then send the latency to a central dynamodb table in the us-west-2 region. To deploy the corresponding table please use `logging_table_setup.sh`. To deploy the functions to all regions use `aws_ping/deploy.sh` and to remove them use `aws_ping/remove.sh`.

## Questions we need to consider

- How do we handle opt-in regions in AWS?
- Do we need to deploy the latency tracker in all regions or can we just deploy it in the regions that we want to track the latency for?
- If we can deploy it in only some regions, how do we know which regions we need to deploy it in?
- Do we need to deploy it in all regions and then just not use the data from the regions that we don't want to track the latency for?
- How "expensive" is it to deploy the latency tracker in all regions?
- Do we want another component to calculate the averages?
- In what time intervals do we want to calculate the averages / get the latency data?
