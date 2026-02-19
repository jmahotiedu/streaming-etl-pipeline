# Reality Check (2026-02-19)

| Claim | Evidence | Status | Next Action |
|---|---|---|---|
| Full managed cloud path is live (MSK + EMR + MWAA + Redshift) | `scripts/deploy.sh` preflight guard documents and enforces entitlement checks for EMR/MWAA/Redshift | Partial | Keep full-path status labeled as pending until account subscription enablement is completed |
| Core mode can still provide a presentable public experience | Terraform now includes ECS Fargate + ALB shell resources (`terraform/dashboard_shell.tf`) and dashboard image build flow (`scripts/deploy.sh`) | Verified (code) | Run `DEPLOY_CORE_ONLY=true ./scripts/deploy.sh` and publish resulting ALB URL |
| Public shell behavior is clearly labeled when warehouse path is unavailable | Streamlit banner and sidebar mode labels in `src/dashboard/app.py` show `Core Mode / Demo Data` in core deployments | Verified | Keep `scripts/shell-smoke.sh` in deployment verification |
| Deployment checks only validate infrastructure liveness | Added shell smoke check (`scripts/shell-smoke.sh`) plus existing core smoke check (`scripts/core-smoke.sh`) | Verified | Run both smoke checks after each cloud apply |
| External API keys are required for demo functionality | No third-party API key dependency for shell flow; AWS credentials are required for deploy and optional `REDSHIFT_ADMIN_PASSWORD` is only required for full path | Verified | Keep docs explicit to avoid confusion about missing API keys |
| Runtime docs contain unresolved TODO/FIXME placeholders | Cross-repo scan on `2026-02-19` found no unresolved TODO/FIXME markers in runtime code/docs | Verified | Keep placeholder scans in release checklist |
