{{ config(
    materialized='incremental',
    unique_key='txn_id'
) }}

with raw_data as (
    -- Pastikan nama source ni SAMA macam dalam src_transactions.yml kau
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

    {{ audit_columns('bronze') }},
    'CLEAN' as _record_status,
    cast(null as string) as _source_file

from raw_data

{% if is_incremental() %}
  -- Logic incremental untuk elak duplicate batch
  where txn_date > (select max(txn_date) from {{ this }})
{% endif %}