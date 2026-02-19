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
APPLY="${APPLY:-true}"
SKIP_SERVICE_PREFLIGHT="${SKIP_SERVICE_PREFLIGHT:-false}"
DEPLOY_CORE_ONLY="${DEPLOY_CORE_ONLY:-false}"
ENABLE_EMR="${ENABLE_EMR:-true}"
ENABLE_MWAA="${ENABLE_MWAA:-true}"
ENABLE_REDSHIFT="${ENABLE_REDSHIFT:-true}"
ENABLE_DASHBOARD_SHELL="${ENABLE_DASHBOARD_SHELL:-true}"
DASHBOARD_IMAGE_TAG="${DASHBOARD_IMAGE_TAG:-latest}"
BUILD_DASHBOARD_IMAGE="${BUILD_DASHBOARD_IMAGE:-true}"

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

echo "Using account: ${AWS_ACCOUNT_ID}"
echo "State bucket:   ${STATE_BUCKET}"
echo "Lock table:     ${LOCK_TABLE_NAME}"
echo "Environment:    ${TF_ENVIRONMENT}"
echo "Enable EMR:     ${ENABLE_EMR}"
echo "Enable MWAA:    ${ENABLE_MWAA}"
echo "Enable Redshift:${ENABLE_REDSHIFT}"
echo "Enable shell:   ${ENABLE_DASHBOARD_SHELL}"

check_service_access() {
  local service_name="$1"
  shift
  local output
  if ! output="$("$@" 2>&1)"; then
    if [[ "${output}" == *"SubscriptionRequiredException"* ]]; then
      echo "ERROR: ${service_name} is not enabled for account ${AWS_ACCOUNT_ID} in ${AWS_REGION}."
      echo "       Enable ${service_name} subscription/access before APPLY=true."
    else
      echo "ERROR: ${service_name} preflight check failed."
      echo "${output}"
    fi
    return 1
  fi
}

if [[ "${APPLY}" == "true" && "${SKIP_SERVICE_PREFLIGHT}" != "true" ]]; then
  echo "Running AWS service entitlement preflight checks..."
  preflight_failed=false
  if [[ "${ENABLE_EMR}" == "true" ]]; then
    check_service_access "EMR" "${AWS_CLI_BIN}" emr list-clusters --region "${AWS_REGION}" --max-items 1 || preflight_failed=true
  fi
  if [[ "${ENABLE_MWAA}" == "true" ]]; then
    check_service_access "MWAA" "${AWS_CLI_BIN}" mwaa list-environments --region "${AWS_REGION}" --max-results 1 || preflight_failed=true
  fi
  if [[ "${ENABLE_REDSHIFT}" == "true" ]]; then
    check_service_access "Redshift Serverless" "${AWS_CLI_BIN}" redshift-serverless list-workgroups --region "${AWS_REGION}" --max-results 1 || preflight_failed=true
  fi

  if [[ "${preflight_failed}" == "true" ]]; then
    echo "Aborting deploy before terraform apply to avoid partial infrastructure creation."
    echo "Set SKIP_SERVICE_PREFLIGHT=true to bypass this guard."
    exit 1
  fi
fi

if ! "${AWS_CLI_BIN}" s3api head-bucket --bucket "${STATE_BUCKET}" 2>/dev/null; then
  echo "Creating state bucket ${STATE_BUCKET}..."
  if [[ "${AWS_REGION}" == "us-east-1" ]]; then
    "${AWS_CLI_BIN}" s3api create-bucket --bucket "${STATE_BUCKET}" --region "${AWS_REGION}"
  else
    "${AWS_CLI_BIN}" s3api create-bucket \
      --bucket "${STATE_BUCKET}" \
      --region "${AWS_REGION}" \
      --create-bucket-configuration LocationConstraint="${AWS_REGION}"
  fi
  "${AWS_CLI_BIN}" s3api put-bucket-versioning \
    --bucket "${STATE_BUCKET}" \
    --versioning-configuration Status=Enabled
fi

if ! "${AWS_CLI_BIN}" dynamodb describe-table --table-name "${LOCK_TABLE_NAME}" >/dev/null 2>&1; then
  echo "Creating lock table ${LOCK_TABLE_NAME}..."
  "${AWS_CLI_BIN}" dynamodb create-table \
    --table-name "${LOCK_TABLE_NAME}" \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region "${AWS_REGION}" >/dev/null
  "${AWS_CLI_BIN}" dynamodb wait table-exists --table-name "${LOCK_TABLE_NAME}" --region "${AWS_REGION}"
fi

cd "${TF_DIR}"

terraform_vars=(
  "-var=aws_region=${AWS_REGION}"
  "-var=environment=${TF_ENVIRONMENT}"
  "-var=enable_emr=${ENABLE_EMR}"
  "-var=enable_mwaa=${ENABLE_MWAA}"
  "-var=enable_redshift=${ENABLE_REDSHIFT}"
  "-var=enable_dashboard_shell=${ENABLE_DASHBOARD_SHELL}"
  "-var=dashboard_image_tag=${DASHBOARD_IMAGE_TAG}"
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

"${TERRAFORM_BIN}" plan -out=tfplan \
  "${terraform_vars[@]}"

if [[ "${APPLY}" == "true" ]]; then
  "${TERRAFORM_BIN}" apply -auto-approve tfplan
  "${TERRAFORM_BIN}" output

  if [[ "${ENABLE_DASHBOARD_SHELL}" == "true" ]]; then
    DASHBOARD_URL="$("${TERRAFORM_BIN}" output -raw dashboard_shell_url)"
    echo "Dashboard shell URL: ${DASHBOARD_URL}"

    if [[ "${BUILD_DASHBOARD_IMAGE}" == "true" ]]; then
      if ! command -v docker >/dev/null 2>&1; then
        echo "docker binary not found. Skipping dashboard image build/push."
        echo "Set BUILD_DASHBOARD_IMAGE=false to suppress this warning."
      else
        DASHBOARD_REPO="$("${TERRAFORM_BIN}" output -raw dashboard_ecr_repository_url)"
        DASHBOARD_CLUSTER="$("${TERRAFORM_BIN}" output -raw dashboard_ecs_cluster_name)"
        DASHBOARD_SERVICE="$("${TERRAFORM_BIN}" output -raw dashboard_ecs_service_name)"
        ECR_REGISTRY="$(echo "${DASHBOARD_REPO}" | cut -d'/' -f1)"

        echo "Logging into ECR registry ${ECR_REGISTRY}..."
        "${AWS_CLI_BIN}" ecr get-login-password --region "${AWS_REGION}" \
          | docker login --username AWS --password-stdin "${ECR_REGISTRY}" >/dev/null

        echo "Building dashboard image ${DASHBOARD_REPO}:${DASHBOARD_IMAGE_TAG}..."
        docker build \
          -f "${ROOT_DIR}/Dockerfile.dashboard" \
          -t "${DASHBOARD_REPO}:${DASHBOARD_IMAGE_TAG}" \
          "${ROOT_DIR}"

        echo "Pushing dashboard image..."
        docker push "${DASHBOARD_REPO}:${DASHBOARD_IMAGE_TAG}"

        echo "Forcing ECS service redeploy..."
        "${AWS_CLI_BIN}" ecs update-service \
          --cluster "${DASHBOARD_CLUSTER}" \
          --service "${DASHBOARD_SERVICE}" \
          --force-new-deployment \
          --region "${AWS_REGION}" >/dev/null
      fi
    fi
  fi
else
  echo "Skipping apply because APPLY=${APPLY}"
fi
