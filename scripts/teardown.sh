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
DESTROY_STATE_BACKEND="${DESTROY_STATE_BACKEND:-false}"
DEPLOY_CORE_ONLY="${DEPLOY_CORE_ONLY:-false}"
ENABLE_EMR="${ENABLE_EMR:-true}"
ENABLE_MWAA="${ENABLE_MWAA:-true}"
ENABLE_REDSHIFT="${ENABLE_REDSHIFT:-true}"

if [[ "${DEPLOY_CORE_ONLY}" == "true" ]]; then
  ENABLE_EMR="false"
  ENABLE_MWAA="false"
  ENABLE_REDSHIFT="false"
fi

if [[ "${ENABLE_REDSHIFT}" == "true" && -z "${REDSHIFT_ADMIN_PASSWORD:-}" ]]; then
  echo "REDSHIFT_ADMIN_PASSWORD is required when ENABLE_REDSHIFT=true."
  exit 1
fi

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

terraform_vars=(
  "-var=aws_region=${AWS_REGION}"
  "-var=environment=${TF_ENVIRONMENT}"
  "-var=enable_emr=${ENABLE_EMR}"
  "-var=enable_mwaa=${ENABLE_MWAA}"
  "-var=enable_redshift=${ENABLE_REDSHIFT}"
)

if [[ "${ENABLE_REDSHIFT}" == "true" ]]; then
  terraform_vars+=("-var=redshift_admin_password=${REDSHIFT_ADMIN_PASSWORD}")
fi

"${TERRAFORM_BIN}" init -reconfigure \
  -backend-config="bucket=${STATE_BUCKET}" \
  -backend-config="key=${TF_STATE_KEY}" \
  -backend-config="region=${AWS_REGION}" \
  -backend-config="dynamodb_table=${LOCK_TABLE_NAME}" \
  -backend-config="encrypt=true"

"${TERRAFORM_BIN}" destroy -auto-approve \
  "${terraform_vars[@]}"

if [[ "${DESTROY_STATE_BACKEND}" == "true" ]]; then
  "${AWS_CLI_BIN}" s3 rb "s3://${STATE_BUCKET}" --force || true
  "${AWS_CLI_BIN}" dynamodb delete-table --table-name "${LOCK_TABLE_NAME}" --region "${AWS_REGION}" || true
fi
