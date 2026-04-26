-- Snowflake Notebook cell (SQL) starter for training.
--
-- Recommended: create a Snowflake Notebook (Python) and use Snowpark ML there for real in-Snowflake training.
-- This file acts as a "bootstrap" for the objects and a reference for where to read/write.
--
-- Expected objects:
--   - RAW.STOP_OBSERVATIONS (raw labels)
--   - MART.MODEL_RUNS (model registry)
--   - MART.MODEL_STAGE (internal stage for model artifacts)
--
-- One-time setup:
CREATE STAGE IF NOT EXISTS MART.MODEL_STAGE;

-- (Optional) Ensure model registry table exists (Terraform also creates it):
CREATE TABLE IF NOT EXISTS MART.MODEL_RUNS (
  MODEL_ID     STRING,
  TRAINED_AT   TIMESTAMP_NTZ,
  ALGO         STRING,
  MAE          FLOAT,
  RMSE         FLOAT,
  FEATURES     VARIANT,
  ARTIFACT_URI STRING,
  IS_ACTIVE    BOOLEAN
);

-- Notes for the Python notebook:
-- 1) Read training data from RAW.STOP_OBSERVATIONS (or MART.MODEL_FEATURES if you populate it).
-- 2) Feature engineering: either replicate via_common.features logic in Snowpark, or materialize MART.MODEL_FEATURES.
-- 3) Train with Snowpark ML; serialize model with joblib/pickle.
-- 4) Write artifact to @MART.MODEL_STAGE/<model_id>.joblib
-- 5) Mark previous runs inactive and insert a new active row into MART.MODEL_RUNS.
