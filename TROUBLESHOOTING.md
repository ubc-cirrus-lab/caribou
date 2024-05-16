#  Troubleshooting

This document contains common issues and solutions for the Caribou project.

##  AWS Lambda

Interesting findings and quirks with working with AWS Lambda and Chalice

- To make your life easier before working on our project once run `aws configure` to setup your account (you will need your AWS access key and secret key). This will create a `~/.aws/credentials` file which will be used by the AWS CLI to authenticate you. You can also set the default region to `us-west-2` (Oregon) which is the region we are using as home region for our project due to its close proximity to Vancouver.

- AWS Lambda sends its logs to CloudWatch, which is generally horrible to display errors. To actually find the logs we are looking for go to Logs Insights and run the following query:

    ```sql
    filter @message LIKE /ERROR/ or @message LIKE /Task timed out/
    ```

- Timeouts: AWS Lambda by default has a limit of 60s which should be more than enough for most of our tasks. However, for example the ping function requires more. To increase the timeout in the code go to the chalice `config.json` file and add the following:

    ```json
    "lambda_timeout" : 120
    ```

- If you are deploying an application and you are getting such an error:

    ```bash
    The security token included in the request is invalid.
    ```

  Firstly check if the region you are deploying to (e.g. set by your environment variable `AWS_DEFAULT_REGION`) is an enabled region for AWS Lambda and not an opt-in region that we haven't enabled yet in our project.

  If this is not the case check if your AWS security token is correctly set using `aws configure` and also check that there is no environment variable set which may take precedence. If this is not the case, then you might have a timed out token in your `~/.aws/credentials` file. To fix this, simply delete the file and run `aws configure` again.

- Generally it is easier to do things like DynamoDB table setups in bash scripts rather than in Python simply because it's less verbose. Here is a template:

    ```bash
    #!/bin/bash

    TABLE_NAME="YOUR_TABLE_NAME"
    REGION="YOUR_REGION"

    # Check if the table exists
    TABLE_EXISTS=$(aws dynamodb describe-table --table-name $TABLE_NAME --region $REGION)

    if [ -z "$TABLE_EXISTS" ]; then
        # Create the table if it doesn't exist
        aws dynamodb create-table \
            --table-name $TABLE_NAME \
            --attribute-definitions \
                AttributeName=attributeA,AttributeType=S \
                AttributeName=attributeB,AttributeType=S \
            --key-schema \
                AttributeName=attributeA,KeyType=HASH \
                AttributeName=attributeB,KeyType=RANGE \
            --provisioned-throughput \
                ReadCapacityUnits=10,WriteCapacityUnits=10 \
            --region $REGION
    fi
    ```

    In a future version we would probably want a more generic script that can be used for any table. But for now this is fine.
