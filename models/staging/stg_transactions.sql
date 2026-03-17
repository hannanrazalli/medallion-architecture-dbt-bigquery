{{ config(
    materialized='incremental',
    unique_key='txn_id'
) }}

with raw_data as (
    select * from {{ source('transactions_source', 'transactions_batch_1') }}
)

select
    txn_id,
    cust_id,
    amount,
    is_member,
    points,
    status,
    txn_date,

    -- Audit Columns
    current_timestamp() as _ingest_at,
    current_date() as _ingest_date,
    '{{ invocation_id }}' as _batch_id_bronze,
    
    -- GANTI LOGIC NI: Kita check dulu column tu wujud ke tak
    -- Kalau kau upload manual, selalunya memang tak wujud, so kita letak 'CLEAN' terus
    'CLEAN' as _record_status

from raw_data

{% if is_incremental() %}
  where txn_date > (select max(txn_date) from {{ this }})
{% endif %}