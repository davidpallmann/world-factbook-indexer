# AWS Transform Deployment Scripts for .NET Applications

## Overview

As part of the code transformation AWS Transform analyzed your source code repository to identify deployable .NET applications, including:

* ASP.NET MVC applications
* ASP.NET Web applications

### Deployable Applications Found

| Project name | Solution name | Project file location | Deployment Scripts and templates location |
|--------------|---------------|----------------------|---------------------------|
| No deployable applications found | | | |

AWS Transform provides deployment scripts and AWS CloudFormation templates to help you deploy your transformed .NET applications to Amazon EC2 instances. This comprehensive deployment solution includes infrastructure provisioning, application deployment, and management tools that streamline the process of getting your applications running in the AWS cloud.

The generated deployment assets are placed in directory `{Project Directory}/aws-transform-deploy/` for each project in the table above.

## Deployment workflow

The deployment process follows these key steps:

1. Core Infrastructure Setup
   - Creates required IAM Roles and Instance Profiles for secure access.
   - Provisions S3 bucket for storing deployment artifacts.
   - Sets up SSM parameters for configuration management.

2. Application Infrastructure 
   - Deploys application-specific infrastructure resources.
   - Configures networking, load balancers, auto-scaling etc.
   - Sets up monitoring and logging.

3. Application Deployment
   - Uploads application packages to S3.
   - Deploys applications using the provisioned infrastructure.
   - Validates deployment health.

## Core Infrastructure Setup

### Important: Administrator privileges are needed
To execute templates in this directory, you need admin-level permissions to create IAM roles. If you don't have these permissions, please forward contents of this directory to your AWS account administrator for review and implementation.

### Prerequisites
1. Admin-level permissions on your AWS account to manage IAM Roles, IAM Instance Profiles, and CloudFormation stacks.
2. AWS credentials properly configured via either:
   - Environment variables (e.g. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)
   - AWS credentials file (`~/.aws/credentials`)
   See https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html for detailed setup instructions
3. **Review the CloudFormation template** `iam_roles_template.yml` before deployment.

### Create IAM Roles Using AWS CLI

Use `iam_roles_template.yml` CloudFormation template to create deployment roles:

1. AWSTransformDotNET-Infra-Deployment-Role: a role used to deploy infrastructure. Has permissions to create CloudFormation stacks, EC2 instances.
2. AWSTransformDotNET-Application-Deployment-Role: a role used to deploy applications. Has permissions to write to S3 bucket and execute SSM commands on a given instance.
3. AWSTransformDotNET-EC2-Instance-Role: a role used by EC2 instance to access S3 bucket.


#### Template Parameters

The `iam_roles_template.yml` CloudFormation template accepts the following parameters:

| Parameter | Type | Description | Default |
|-----------|------|-------------|----------|
| `S3BucketName` | String | Existing S3 bucket name for storing deployment artifacts. If empty, a new bucket will be created | Empty (creates new bucket) |
| `KMSKeyArn` | String | Optional KMS Key ARN for encryption of deployment artifacts in S3 bucket | Empty (no encryption) |


### Deploy CloudFormation stack to create IAM roles and S3 bucket

```
aws cloudformation deploy --template-file iam_roles_template.yml --stack-name atx-deploy-iam-roles --capabilities CAPABILITY_NAMED_IAM --tags CreatedFor=AWSTransformDotNET
```
### Deploy CloudFormation stack with parameters
```
aws cloudformation deploy --template-file iam_roles_template.yml --stack-name atx-deploy-iam-roles --capabilities CAPABILITY_NAMED_IAM --parameter-overrides S3BucketName=my-deployment-bucket KMSKeyArn=arn:aws:kms:region:account:key/key-id --tags CreatedFor=AWSTransformDotNET
```

### Check stack status and outputs
```
aws cloudformation describe-stacks --stack-name atx-deploy-iam-roles --query 'Stacks[0].StackStatus' --output text
```


### SSM Parameter Created

The template creates the following SSM parameter:

- **Parameter Name**: `/transform/bucket-name`
- **Purpose**: Stores the S3 bucket name used for deployments
- **Usage**: Deployment scripts automatically read this parameter to determine where to upload application packages
- **Value**: Either the provided `S3BucketName` parameter or the name of the newly created bucket

## Assigning users to the roles

After the IAM roles are created, the administrator needs to edit the trust policy for the following roles
to specify which users/roles can assume them for infrastructure and application deployment:
1. AWSTransformDotNET-Infra-Deployment-Role
2. AWSTransformDotNET-Application-Deployment-Role

### Example trust policy to allow specific IAM users/roles to assume the deployment role:
```
aws iam update-assume-role-policy --role-name <Role Name> --policy-document '{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\": [\"arn:aws:iam::ACCOUNT_ID:user/USERNAME\", \"arn:aws:iam::ACCOUNT_ID:role/ROLENAME\"]},\"Action\":\"sts:AssumeRole\"}]}'
```

# Deploy your transformed applications

Refer to `README.md` file in `Deployment Scripts and templates location` for each individual application as in the table above.