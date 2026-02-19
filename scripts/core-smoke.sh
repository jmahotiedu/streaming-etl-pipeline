#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT_DIR}/terraform"

AWS_REGION="${AWS_REGION:-us-east-1}"
TF_ENVIRONMENT="${TF_ENVIRONMENT:-demo}"
TF_STATE_KEY="${TF_STATE_KEY:-streaming-etl-pipeline/${TF_ENVIRONMENT}/terraform.tfstate}"
LOCK_TABLE_NAME="${LOCK_TABLE_NAME:-streaming-etl-tf-lock-${TF_ENVIRONMENT}}"
TERRAFORM_BIN="${TERRAFORM_BIN:-terraform}"
AWS_CLI_BIN="${AWS_CLI_BIN:-aws}"

if ! command -v "${AWS_CLI_BIN}" >/dev/null 2>&1; then
  echo "aws CLI binary '${AWS_CLI_BIN}' not found."
  exit 1
fi

if ! command -v "${TERRAFORM_BIN}" >/dev/null 2>&1; then
  echo "Terraform binary '${TERRAFORM_BIN}' not found."
  exit 1
fi

AWS_ACCOUNT_ID="$("${AWS_CLI_BIN}" sts get-caller-identity --query Account --output text)"
STATE_BUCKET="${STATE_BUCKET:-streaming-etl-tf-state-${AWS_ACCOUNT_ID}-${AWS_REGION}}"

cd "${TF_DIR}"

"${TERRAFORM_BIN}" init -reconfigure \
  -backend-config="bucket=${STATE_BUCKET}" \
  -backend-config="key=${TF_STATE_KEY}" \
  -backend-config="region=${AWS_REGION}" \
  -backend-config="dynamodb_table=${LOCK_TABLE_NAME}" \
  -backend-config="encrypt=true" >/dev/null

MSK_ARN="$("${TERRAFORM_BIN}" output -raw msk_cluster_arn)"
MSK_BOOTSTRAP="$("${TERRAFORM_BIN}" output -raw msk_bootstrap_servers)"
BRONZE_BUCKET="$("${TERRAFORM_BIN}" output -raw s3_bronze_bucket)"
SILVER_BUCKET="$("${TERRAFORM_BIN}" output -raw s3_silver_bucket)"
GOLD_BUCKET="$("${TERRAFORM_BIN}" output -raw s3_gold_bucket)"

echo "MSK cluster ARN: ${MSK_ARN}"
echo "MSK bootstrap:   ${MSK_BOOTSTRAP}"
echo "S3 bronze:       ${BRONZE_BUCKET}"
echo "S3 silver:       ${SILVER_BUCKET}"
echo "S3 gold:         ${GOLD_BUCKET}"

MSK_STATE="$("${AWS_CLI_BIN}" kafka describe-cluster-v2 --cluster-arn "${MSK_ARN}" --region "${AWS_REGION}" --query "ClusterInfo.State" --output text)"
echo "MSK state:       ${MSK_STATE}"

if [[ "${MSK_STATE}" != "ACTIVE" ]]; then
  echo "MSK is not ACTIVE yet. Wait for creation to finish before running producer workloads."
  exit 1
fi

TMP_FILE="$(mktemp)"
MARKER_KEY="smoke/core-smoke-$(date -u +%Y%m%dT%H%M%SZ).txt"
echo "core-smoke ${MARKER_KEY}" > "${TMP_FILE}"

"${AWS_CLI_BIN}" s3 cp "${TMP_FILE}" "s3://${BRONZE_BUCKET}/${MARKER_KEY}" --region "${AWS_REGION}" >/dev/null
"${AWS_CLI_BIN}" s3 rm "s3://${BRONZE_BUCKET}/${MARKER_KEY}" --region "${AWS_REGION}" >/dev/null
rm -f "${TMP_FILE}"

echo "Core smoke passed: MSK active and S3 write/delete check succeeded."
echo "Next: run producer/consumer from inside the VPC (ECS, EC2, or Cloud9 in VPC)."
