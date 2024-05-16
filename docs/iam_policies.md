# IAM Policies

There are two types of IAM policies required for Caribou workflows:

1. **Workflow IAM Policy**: This policy is required by a workflow minimally to be able to execute giving it permissions for logging, SNS, and dynamoDB to retrieve the latest deployment.
2. **Developer IAM Policy**: This policy is required by the developer to be able to deploy the workflow. This policy should have permissions to create and manage the resources required by the workflow.

##  AWS

###  Workflow IAM Policy

The minimal AWS IAM policy required for a workflow is as follows:

```json
"aws": {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Action": [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource": "arn:aws:logs:*:*:*",
        "Effect": "Allow"
      },
      {
        "Action": ["sns:Publish"],
        "Resource": "arn:aws:sns:*:*:*",
        "Effect": "Allow"
      },
      {
        "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem"],
        "Resource": "arn:aws:dynamodb:*:*:*",
        "Effect": "Allow"
      }
    ]
  }
```

###  Developer IAM Policy

The minimal AWS IAM policy required for a developer to deploy a workflow is as follows:

```json
{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Action": [
    "logs:CreateLogGroup",
    "logs:CreateLogStream",
    "logs:PutLogEvents"
   ],
   "Resource": "arn:aws:logs:*:*:*",
   "Effect": "Allow"
  },
  {
   "Action": [
    "sns:Publish"
   ],
   "Resource": "arn:aws:sns:*:*:*",
   "Effect": "Allow"
  },
  {
   "Action": [
    "dynamodb:GetItem",
    "dynamodb:UpdateItem"
   ],
   "Resource": "arn:aws:dynamodb:*:*:*",
   "Effect": "Allow"
  },
  {
   "Action": [
    "s3:GetObject",
    "s3:PutObject"
   ],
   "Resource": "arn:aws:s3:::*",
   "Effect": "Allow"
  },
  {
   "Sid": "Statement1",
   "Effect": "Allow",
   "Action": [
    "iam:AttachRolePolicy",
    "iam:CreateRole",
    "iam:CreatePolicy",
    "iam:PutRolePolicy",
    "ecr:CreateRepository",
    "ecr:GetAuthorizationToken",
    "ecr:InitiateLayerUpload",
    "ecr:UploadLayerPart",
    "ecr:CompleteLayerUpload",
    "ecr:BatchGetImage",
    "ecr:BatchCheckLayerAvailability",
    "ecr:DescribeImages",
    "ecr:DescribeRepositories",
    "ecr:GetDownloadUrlForLayer",
    "ecr:ListImages",
    "ecr:PutImage",
    "ecr:SetRepositoryPolicy",
    "ecr:GetRepositoryPolicy",
    "ecr:DeleteRepository",
    "lambda:GetFunction",
    "lambda:AddPermission",
    "pricing:ListPriceLists",
    "pricing:GetPriceListFileUrl",
    "logs:FilterLogEvents"
   ],
   "Resource": "*"
  },
  {
   "Sid": "Statement2",
   "Effect": "Allow",
   "Action": [
    "iam:GetRole",
    "iam:PassRole"
   ],
   "Resource": "arn:aws:iam:::role/*"
  }
 ]
}
```
