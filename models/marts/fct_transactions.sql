{{ config(
    materialized='table',
    partition_by={
      "field": "txn_date_key",
      "data_type": "date",
      "granularity": "day"
    }
) }}

with silver_data as (
    select * from {{ ref('int_transactions_refined') }}
    where is_deleted = false
),

final_fact as (
    select
        txn_id,
        cust_id,
        amount,
        points,
        status,
        -- Business Date Key (Partition Key)
        cast(txn_date as date) as txn_date_key,
        -- Audit dari Macro
        {{ audit_columns('gold') }}
    from silver_data
    where amount > 0 
      and cust_id is not null
)

select * from final_fact