provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_admin_user
  role              = var.snowflake_admin_role
  password          = var.snowflake_pat_token
}

resource "snowflake_database" "via_delays" {
  name = var.database_name
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.via_delays.name
  name     = var.raw_schema_name
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.via_delays.name
  name     = var.staging_schema_name
}

resource "snowflake_schema" "mart" {
  database = snowflake_database.via_delays.name
  name     = var.mart_schema_name
}

resource "snowflake_warehouse" "via_delay" {
  name                = var.warehouse_name
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
}

resource "snowflake_account_role" "service" {
  name = var.service_role_name
}

resource "snowflake_user" "service" {
  name                 = var.service_user_name
  password             = var.service_user_password
  default_role         = snowflake_account_role.service.name
  default_warehouse    = snowflake_warehouse.via_delay.name
  default_namespace    = "${snowflake_database.via_delays.name}.${snowflake_schema.raw.name}"
  must_change_password = false
}

resource "snowflake_grant_account_role" "service_user_role" {
  role_name = snowflake_account_role.service.fully_qualified_name
  user_name = snowflake_user.service.fully_qualified_name
}

resource "snowflake_grant_privileges_to_account_role" "service_database_usage" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.via_delays.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "service_raw_schema_usage" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  privileges        = ["USAGE"]

  on_schema {
    schema_name = snowflake_schema.raw.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "service_staging_schema_usage" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  privileges        = ["USAGE"]

  on_schema {
    schema_name = snowflake_schema.staging.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "service_mart_schema_usage" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  privileges        = ["USAGE"]

  on_schema {
    schema_name = snowflake_schema.mart.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "service_warehouse_usage" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.via_delay.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "service_raw_tables_dml" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]

  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.raw.fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "service_raw_future_tables_dml" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]

  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.raw.fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "service_mart_tables_dml" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]

  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.mart.fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "service_mart_future_tables_dml" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]

  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.mart.fully_qualified_name
    }
  }
}

resource "snowflake_table" "raw_stop_observations" {
  database = snowflake_database.via_delays.name
  schema   = snowflake_schema.raw.name
  name     = "STOP_OBSERVATIONS"

  column {
    name = "TRAIN_NUMBER"
    type = "STRING"
  }

  column {
    name = "SERVICE_DATE"
    type = "DATE"
  }

  column {
    name = "STOP_SEQUENCE"
    type = "NUMBER"
  }

  column {
    name = "STATION_CODE"
    type = "STRING"
  }

  column {
    name = "SCHEDULED_ARRIVAL"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "ACTUAL_ARRIVAL"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "SCHEDULED_DEPARTURE"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "ACTUAL_DEPARTURE"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "DELAY_MINUTES"
    type = "FLOAT"
  }

  column {
    name = "SOURCE"
    type = "STRING"
  }

  column {
    name = "SCRAPED_AT"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "PAYLOAD"
    type = "VARIANT"
  }
}

resource "snowflake_table" "raw_job_runs" {
  database = snowflake_database.via_delays.name
  schema   = snowflake_schema.raw.name
  name     = "JOB_RUNS"

  column {
    name = "JOB_ID"
    type = "STRING"
  }

  column {
    name = "KIND"
    type = "STRING"
  }

  column {
    name = "STARTED_AT"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "FINISHED_AT"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "STATUS"
    type = "STRING"
  }

  column {
    name = "ROW_COUNT"
    type = "NUMBER"
  }

  column {
    name = "ERROR"
    type = "STRING"
  }
}

resource "snowflake_table" "raw_weather_observations" {
  database = snowflake_database.via_delays.name
  schema   = snowflake_schema.raw.name
  name     = "WEATHER_OBSERVATIONS"

  column {
    name = "STATION_CODE"
    type = "STRING"
  }

  column {
    name = "OBS_TIME"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "TEMP_C"
    type = "FLOAT"
  }

  column {
    name = "PRECIP_MM"
    type = "FLOAT"
  }

  column {
    name = "SNOW_CM"
    type = "FLOAT"
  }

  column {
    name = "WIND_KPH"
    type = "FLOAT"
  }

  column {
    name = "CONDITION"
    type = "STRING"
  }
}

resource "snowflake_table" "mart_model_features" {
  database = snowflake_database.via_delays.name
  schema   = snowflake_schema.mart.name
  name     = "MODEL_FEATURES"

  column {
    name = "TRAIN_NUMBER"
    type = "STRING"
  }

  column {
    name = "SERVICE_DATE"
    type = "DATE"
  }

  column {
    name = "ORIGIN_STATION"
    type = "STRING"
  }

  column {
    name = "DEST_STATION"
    type = "STRING"
  }

  column {
    name = "DOW"
    type = "NUMBER"
  }

  column {
    name = "IS_WEEKEND"
    type = "BOOLEAN"
  }

  column {
    name = "MONTH"
    type = "NUMBER"
  }

  column {
    name = "HOUR"
    type = "NUMBER"
  }

  column {
    name = "TRAIN_NUMBER_HASH"
    type = "NUMBER"
  }

  column {
    name = "AVG_DELAY_L30D"
    type = "FLOAT"
  }

  column {
    name = "AVG_DELAY_L30D_DOW"
    type = "FLOAT"
  }

  column {
    name = "WEATHER_PRECIP_MM"
    type = "FLOAT"
  }

  column {
    name = "WEATHER_SNOW_CM"
    type = "FLOAT"
  }

  column {
    name = "WEATHER_TEMP_C"
    type = "FLOAT"
  }

  column {
    name = "TARGET_DELAY_MIN"
    type = "FLOAT"
  }
}

resource "snowflake_table" "mart_model_runs" {
  database = snowflake_database.via_delays.name
  schema   = snowflake_schema.mart.name
  name     = "MODEL_RUNS"

  column {
    name = "MODEL_ID"
    type = "STRING"
  }

  column {
    name = "TRAINED_AT"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "ALGO"
    type = "STRING"
  }

  column {
    name = "MAE"
    type = "FLOAT"
  }

  column {
    name = "RMSE"
    type = "FLOAT"
  }

  column {
    name = "FEATURES"
    type = "VARIANT"
  }

  column {
    name = "ARTIFACT_URI"
    type = "STRING"
  }

  column {
    name = "IS_ACTIVE"
    type = "BOOLEAN"
  }
}

resource "snowflake_stage" "model_stage" {
  name     = "MODEL_STAGE"
  database = snowflake_database.via_delays.name
  schema   = snowflake_schema.mart.name
}

resource "snowflake_grant_privileges_to_account_role" "service_model_stage_usage" {
  account_role_name = snowflake_account_role.service.fully_qualified_name
  # Internal stage doesn't support USAGE grants; grant READ/WRITE only.
  privileges = ["READ", "WRITE"]

  on_schema_object {
    object_type = "STAGE"
    object_name = snowflake_stage.model_stage.fully_qualified_name
  }
}

locals {
  # Notebook-driven training. No Snowflake stored procedure is managed by Terraform.
  # See: infra/snowflake/notebooks/train_delay_model_notebook.py
  _notebook_training_enabled = true
}

### No stored procedure is managed by Terraform in notebook-driven training mode.
