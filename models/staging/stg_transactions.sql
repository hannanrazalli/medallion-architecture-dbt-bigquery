{{ config(
    materialized='incremental',
    unique_key='txn_id'
) }}

WITH raw_data AS (
    SELECT *
    FROM {{ source('transactions_source', 'transactions_batch_1') }}
)

SELECT
    txn_id,
    cust_id,
    amount,
    points,
    is_member,
    status,
    txn_date,
    {{ audit_columns('bronze') }},
    CASE
        WHEN txn_id IS NULL OR txn_id = '' THEN 'CORRUPT'
        WHEN cust_id IS NULL OR cust_id < 0 THEN 'CORRUPT'
        WHEN amount IS NULL OR amount < 0 THEN 'CORRUPT'
        WHEN points IS NULL OR points < 0 THEN 'CORRUPT'
        WHEN is_member IS NULL THEN 'CORRUPT'
        WHEN txn_date IS NULL THEN 'CORRUPT'
        ELSE 'CLEAN'
    END AS _record_status
FROM raw_data

{% if is_incremental() %}
  WHERE _ingest_at > (SELECT timestamp_sub(max(_ingest_at), INTERVAL 1 HOUR) FROM {{ this }})
{% endif %}