# Datacenter Trackers

This folder contains the datacenter trackers for the multi-x-serverless project.

The datacenter trackers track the existence of a datacenter and maintain a list of all datacenters that are currently available in the dynamodb table `multi-x-serverless-datacenter-info`.

This should run very infrequently (e.g. every 10 days or less) and it is enough to run it in one region only.
