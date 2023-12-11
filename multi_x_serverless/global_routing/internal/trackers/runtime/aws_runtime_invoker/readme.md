# CD into directory
cd multi_x_serverless/global_routing/internal/trackers/runtime/aws_runtime_invoker

# Give permission to execute script
poetry run chmod +x ./deploy.sh

# Deploy function on all regions except for select few
poetry run ./deploy.sh