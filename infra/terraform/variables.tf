variable "snowflake_organization_name" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name within the organization"
  type        = string
}

variable "snowflake_admin_user" {
  description = "Admin user used by Terraform to provision resources"
  type        = string
}

variable "snowflake_pat_token" {
  description = "Snowflake PAT token used as password for Terraform provider"
  type        = string
  sensitive   = true
}

variable "snowflake_admin_role" {
  description = "Admin role used by Terraform (for example: ACCOUNTADMIN or SECURITYADMIN)"
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "database_name" {
  description = "Primary database name"
  type        = string
  default     = "VIA_DELAYS"
}

variable "raw_schema_name" {
  description = "RAW schema name"
  type        = string
  default     = "RAW"
}

variable "staging_schema_name" {
  description = "STAGING schema name"
  type        = string
  default     = "STAGING"
}

variable "mart_schema_name" {
  description = "MART schema name"
  type        = string
  default     = "MART"
}

variable "warehouse_name" {
  description = "Warehouse used by ingestion/training/prediction services"
  type        = string
  default     = "VIA_DELAY_WH"
}

variable "service_role_name" {
  description = "Role used by application services"
  type        = string
  default     = "VIA_DELAY_ROLE"
}

variable "service_user_name" {
  description = "Service user for app connections"
  type        = string
  default     = "VIA_DELAY_USER"
}

variable "service_user_password" {
  description = "Service user password"
  type        = string
  sensitive   = true
}
