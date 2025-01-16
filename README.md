# Amazon Q Developer Subscription Cost Breakdown



Oragnizations need an effective way to chargeback Amazon Q Developer subscription charges. These charges are accumulated at the payer level and the large enterprises need to distribute those charges to the corresponding business units. 

The routine provided will query the CUR data using Athena to identify the Q developer subscription charges grouped by the user GUID. It will then look up the user information from IAM IDC and store the cost and the user email in a DynamoDB table.

![alt text](Q-Developer-Chargeback.png "Q Developer Chargeback")

# Prerequisites
1.	At the payer level, verify if an [AWS CUR 2.0](https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2.html) exist in AWS Data Exports. If not, create one with the Include resource IDs option selected.
2. Setup CUR querying using Amazon Athena following the [instructions](https://docs.aws.amazon.com/cur/latest/userguide/cur-query-athena.html).  
3. Create an [AWS Batch job queue](https://docs.aws.amazon.com/batch/latest/userguide/create-job-queue.html) if one does not exist to run the subscription cost analysis as a containerized job. 
# Installation
After cloning the repo:
1. Deploy the CloudFormation template using ``template.yaml``.

    Provide values for the following parameters:

    ### Athena Query Configuration
    - **DatabaseName**: The name of the Athena database (Default: athena_cur)
    - **AthenaTableName**: The name of the Athena table (Default: cur_report)
    - **WorkGroup**: The Athena workgroup to use (Default: primary)
    - **AthenaResultsBucket**: The S3 bucket where Athena query results will be stored
    - **CURDataBucket**: The S3 bucket containing the Cost and Usage Report (CUR) data

    ### IAM IDC Configuration
    - **IDCStoreId**: The IAM Identity Center (IDC) Store ID
    - **IDCCostCenterAttributeName**: The attribute name in IAM IDC that represents the cost center for subscription charges

    ### DynamoDB Configuration
    - **DDBTableName**: The name of the DynamoDB table to store cost data (Default: q-developer-subscription-cost-by-user)

2. Build the Docker container and push it to ECR


```
#!/bin/bash
AWS_ACCOUNT={your-aws-account}
AWS_REGION={your-region}
ECR_REPO=q-dev-cost-analyzer

#Build the container
docker build -t ${ECR_REPO} .

#Tag and Push the container to ECR
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com
docker tag ${ECR_REPO}:latest ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:latest
docker push ${AWS_ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:latest

```
3. Run the batch job

```
aws batch submit-job \
    --job-name q-dev-cost-analysis \
    --job-queue {your-job-queue} \
    --job-definition q-dev-cost-analyzer \
    --parameters '{"year":"{yyyy}","month":"{mm}"}'

```

# Cleanup

Delete the CloudFormation Template