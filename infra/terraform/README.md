# Snowflake Terraform Setup

This Terraform config provisions the baseline Snowflake resources for this project:

- Database: `VIA_DELAYS`
- Schemas: `RAW`, `STAGING`, `MART`
- Warehouse: `VIA_DELAY_WH`
- Service role: `VIA_DELAY_ROLE`
- Service user: `VIA_DELAY_USER`
- Core tables from `infra/snowflake/001_raw.sql` and `infra/snowflake/003_mart.sql`

## Files

- `versions.tf` provider and Terraform version constraints
- `variables.tf` input variables
- `main.tf` Snowflake resources
- `terraform.tfvars.example` sample values

## Usage

1. Copy the example vars file:

```bash
cp terraform.tfvars.example terraform.tfvars
```

2. Fill in admin PAT settings in `terraform.tfvars`.
   - `snowflake_organization_name`
   - `snowflake_account_name`
   - `snowflake_admin_user`
   - `snowflake_pat_token`

3. Initialize and apply:

```bash
terraform init
terraform plan
terraform apply
```

## Mapping to infra/.env.example

- `SNOWFLAKE_ACCOUNT` -> split into `snowflake_organization_name` and `snowflake_account_name`
- `SNOWFLAKE_USER` -> `service_user_name`
- `SNOWFLAKE_PASSWORD` -> `service_user_password`
- `SNOWFLAKE_ROLE` -> `service_role_name`
- `SNOWFLAKE_WAREHOUSE` -> `warehouse_name`
- `SNOWFLAKE_DATABASE` -> `database_name`
- `SNOWFLAKE_SCHEMA_RAW` -> `raw_schema_name`
- `SNOWFLAKE_SCHEMA_STAGING` -> `staging_schema_name`
- `SNOWFLAKE_SCHEMA_MART` -> `mart_schema_name`

## Notes

- Staging views from `infra/snowflake/002_staging.sql` are not created here because they use SQL view definitions with QUALIFY and DENSE_RANK expressions. Keep applying that SQL file as part of your migration process.
- Terraform authentication is configured as Snowflake PAT auth by passing the PAT as the provider password field.
- The Terraform provider uses `organization_name` + `account_name` (new format) rather than legacy account locator.
- Prefer setting the PAT in an environment variable (`SNOWFLAKE_TOKEN`) and omitting `snowflake_pat_token` from committed files.
- Use a privileged admin role such as `ACCOUNTADMIN` for initial bootstrap. After provisioning, app services should connect with the service user and role.
