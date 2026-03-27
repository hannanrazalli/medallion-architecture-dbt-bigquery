{{ config(
    materialized='incremental',
    unique_key='txn_id'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
)

SELECT *
FROM source_data
WHERE _record_status = 'CORRUPT'
  OR amount IS NULL
  OR points IS NULL