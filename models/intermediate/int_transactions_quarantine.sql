{{ config(
    materialized='incremental',
    unique_key='txn_id',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE _ingest_at > (
            SELECT timestamp_sub(max(_ingest_at), INTERVAL 1 HOUR)
            FROM {{ this }})
    {% endif %}
),

deduplicate AS (
    SELECT *
    FROM source_data
    qualify row_number() over(
        partition by txn_id
        order by _ingest_at desc
    ) = 1
),

final_staged AS (
    SELECT
        txn_id,
        cust_id,
        amount,
        points,
        is_member,
        CASE
            WHEN upper(trim(status)) IN ('COMPLETED', 'PENDING', 'CANCELLED') THEN upper(trim(status))
            ELSE 'UNKNOWN'
        END AS status,
        txn_date,
        (upper(trim(status)) = 'CANCELLED') AS is_deleted,
        _record_status,
        _ingest_at,
        _batch_id_bronze,
        {{ audit_columns('silver') }}
    FROM deduplicate
)

SELECT *
FROM final_staged
WHERE amount IS NOT NULL
  OR points IS NOT NULL
  OR _record_status = 'CORRUPT'