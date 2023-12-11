
# CD into directory
cd multi_x_serverless/deployment/applications/fpo-io

# Give permission to execute script
poetry run chmod +x ./deploy.sh

# Deploy function on all regions except for select few
poetry run ./deploy.sh