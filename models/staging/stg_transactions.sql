-- models/staging/stg_transactions.sql

WITH source AS (
    SELECT * FROM {{ source('transactions_source', 'transactions_batch_1') }}
)

SELECT
    -- 1. Casting & Schema (Macam Phase 1 Spark kau)
    CAST(txn_id AS STRING) AS txn_id,
    CAST(cust_id AS INT64) AS cust_id,
    CAST(amount AS FLOAT64) AS amount,
    CAST(is_member AS BOOL) AS is_member,
    CAST(points AS INT64) AS points,
    CAST(status AS STRING) AS status,
    CAST(txn_date AS TIMESTAMP) AS txn_timestamp,

    -- 2. Audit Columns (Ganti logic audit_col Spark kau)
    CURRENT_TIMESTAMP() AS _ingest_at,
    '{{ invocation_id }}' AS _batch_id_bronze, -- ID unik setiap kali dbt run
    CURRENT_DATE() AS _ingest_date,
    
    -- 3. Record Status (Logic is_corrupt Spark kau)
    -- Kita check kalau ID null, maksudnya record tu bermasalah
    CASE 
        WHEN txn_id IS NULL THEN 'CORRUPT'
        ELSE 'CLEAN' 
    END AS _record_status

FROM source