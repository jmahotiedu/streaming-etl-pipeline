# Teardown Guide

Step-by-step instructions for cleaning up all AWS resources provisioned by this pipeline.

## 1. Terraform Destroy

The primary cleanup method. This removes all Terraform-managed resources.

```bash
cd terraform

# Preview what will be destroyed
terraform plan -destroy

# Execute destruction
terraform destroy
```

Type `yes` when prompted. This removes:
- MSK cluster (`streaming-etl-kafka`)
- EMR cluster (`streaming-etl-spark`)
- S3 buckets (Bronze, Silver, Gold)
- Redshift Serverless namespace and workgroup
- MWAA environment
- ECR repositories
- VPC, subnets, security groups, IAM roles

Estimated time: 15-30 minutes (MSK and MWAA are slow to delete).

## 2. Manual S3 Data Cleanup

Terraform cannot destroy non-empty S3 buckets by default. If `terraform destroy` fails on S3:

```bash
# List all pipeline buckets
aws s3 ls | grep pipeline

# Empty and delete each bucket
aws s3 rb s3://pipeline-bronze-dev --force
aws s3 rb s3://pipeline-silver-dev --force
aws s3 rb s3://pipeline-gold-dev --force

# Remove Terraform state bucket (if desired)
aws s3 rb s3://streaming-etl-tf-state --force
```

## 3. CloudWatch Logs Cleanup

Terraform may not clean up all CloudWatch log groups:

```bash
# List pipeline-related log groups
aws logs describe-log-groups --log-group-name-prefix "/aws/msk/" --query 'logGroups[].logGroupName'
aws logs describe-log-groups --log-group-name-prefix "/aws/emr/" --query 'logGroups[].logGroupName'
aws logs describe-log-groups --log-group-name-prefix "/aws/mwaa/" --query 'logGroups[].logGroupName'

# Delete each log group
aws logs delete-log-group --log-group-name /aws/msk/streaming-etl-kafka
aws logs delete-log-group --log-group-name /aws/emr/streaming-etl-spark
aws logs delete-log-group --log-group-name /aws/mwaa/streaming-etl-airflow
```

## 4. ECR Image Cleanup

If Docker images were pushed to ECR:

```bash
# List images
aws ecr list-images --repository-name streaming-etl-producer
aws ecr list-images --repository-name streaming-etl-spark

# Delete all images in a repository
aws ecr batch-delete-image \
  --repository-name streaming-etl-producer \
  --image-ids "$(aws ecr list-images --repository-name streaming-etl-producer --query 'imageIds[*]' --output json)"
```

## 5. Terraform State Cleanup

If using remote state in S3:

```bash
# Remove the state lock DynamoDB table
aws dynamodb delete-table --table-name streaming-etl-tf-lock

# Remove the state bucket
aws s3 rb s3://streaming-etl-tf-state --force
```

## 6. Local Docker Cleanup

```bash
cd streaming-etl-pipeline

# Stop and remove all containers and volumes
docker-compose down -v

# Remove any orphaned volumes
docker volume prune -f

# Remove pipeline images (optional)
docker rmi $(docker images | grep streaming-etl | awk '{print $3}') 2>/dev/null || true
```

## 7. Cost Verification

After teardown, verify no resources are incurring charges:

1. Open AWS **Cost Explorer**: https://console.aws.amazon.com/cost-management/home
2. Filter by the last 24 hours
3. Group by **Service** and check for:
   - Amazon MSK
   - Amazon EMR
   - Amazon S3
   - Amazon Redshift
   - Amazon MWAA
   - Amazon ECR
   - Amazon CloudWatch

4. Open **Billing Dashboard**: https://console.aws.amazon.com/billing/home
5. Check "Month-to-Date Spend by Service"
6. Set up a **Billing Alarm** if not already present:
   ```bash
   aws cloudwatch put-metric-alarm \
     --alarm-name "BillingAlarm-10USD" \
     --metric-name EstimatedCharges \
     --namespace AWS/Billing \
     --statistic Maximum \
     --period 21600 \
     --threshold 10 \
     --comparison-operator GreaterThanThreshold \
     --evaluation-periods 1 \
     --alarm-actions arn:aws:sns:us-east-1:ACCOUNT_ID:billing-alerts
   ```

## Checklist

- [ ] `terraform destroy` completed successfully
- [ ] All S3 buckets emptied and deleted
- [ ] CloudWatch log groups deleted
- [ ] ECR images deleted
- [ ] DynamoDB lock table deleted (if using remote state)
- [ ] Local Docker containers and volumes removed
- [ ] Cost Explorer shows no ongoing charges
- [ ] Billing alarm set up for future protection
