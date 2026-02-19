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
MAX_ATTEMPTS="${MAX_ATTEMPTS:-20}"
SLEEP_SECONDS="${SLEEP_SECONDS:-15}"

if ! command -v "${AWS_CLI_BIN}" >/dev/null 2>&1; then
  echo "aws CLI binary '${AWS_CLI_BIN}' not found."
  exit 1
fi

if ! command -v "${TERRAFORM_BIN}" >/dev/null 2>&1; then
  echo "Terraform binary '${TERRAFORM_BIN}' not found."
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl binary not found."
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

DASHBOARD_URL="$("${TERRAFORM_BIN}" output -raw dashboard_shell_url)"
if [[ -z "${DASHBOARD_URL}" || "${DASHBOARD_URL}" == "null" ]]; then
  echo "dashboard_shell_url output is empty. Deploy with enable_dashboard_shell=true first."
  exit 1
fi

DASHBOARD_CLUSTER="$("${TERRAFORM_BIN}" output -raw dashboard_ecs_cluster_name)"
DASHBOARD_SERVICE="$("${TERRAFORM_BIN}" output -raw dashboard_ecs_service_name)"
if [[ -z "${DASHBOARD_CLUSTER}" || "${DASHBOARD_CLUSTER}" == "null" || -z "${DASHBOARD_SERVICE}" || "${DASHBOARD_SERVICE}" == "null" ]]; then
  echo "dashboard ECS outputs are empty. Deploy with enable_dashboard_shell=true first."
  exit 1
fi

TMP_FILE="$(mktemp)"
status_code="000"
attempt=1

while [[ "${attempt}" -le "${MAX_ATTEMPTS}" ]]; do
  status_code="$(curl -sS -L -o "${TMP_FILE}" -w "%{http_code}" "${DASHBOARD_URL}" || true)"
  if [[ "${status_code}" == "200" ]]; then
    break
  fi
  echo "Attempt ${attempt}/${MAX_ATTEMPTS}: ${DASHBOARD_URL} returned ${status_code}; retrying in ${SLEEP_SECONDS}s..."
  sleep "${SLEEP_SECONDS}"
  attempt=$((attempt + 1))
done

if [[ "${status_code}" != "200" ]]; then
  rm -f "${TMP_FILE}"
  echo "Shell smoke failed: ${DASHBOARD_URL} did not return HTTP 200."
  exit 1
fi

task_def_arn="$("${AWS_CLI_BIN}" ecs describe-services \
  --cluster "${DASHBOARD_CLUSTER}" \
  --services "${DASHBOARD_SERVICE}" \
  --region "${AWS_REGION}" \
  --query "services[0].taskDefinition" \
  --output text)"

if [[ -z "${task_def_arn}" || "${task_def_arn}" == "None" ]]; then
  rm -f "${TMP_FILE}"
  echo "Shell smoke failed: could not resolve dashboard task definition ARN."
  exit 1
fi

env_pairs="$("${AWS_CLI_BIN}" ecs describe-task-definition \
  --task-definition "${task_def_arn}" \
  --region "${AWS_REGION}" \
  --query "taskDefinition.containerDefinitions[?name=='dashboard'].environment[].join(':',[name,value])" \
  --output text)"

if ! grep -q "PIPELINE_MODE:core-mode" <<< "${env_pairs}"; then
  rm -f "${TMP_FILE}"
  echo "Shell smoke failed: dashboard task definition is missing PIPELINE_MODE=core-mode."
  exit 1
fi

if ! grep -q "PIPELINE_DATA_MODE:demo" <<< "${env_pairs}"; then
  rm -f "${TMP_FILE}"
  echo "Shell smoke failed: dashboard task definition is missing PIPELINE_DATA_MODE=demo."
  exit 1
fi

rm -f "${TMP_FILE}"

echo "Shell smoke passed: ${DASHBOARD_URL} is reachable and dashboard task env is set for Core Mode / Demo Data."
