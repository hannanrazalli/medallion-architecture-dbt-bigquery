{{ config(
    materialized='incremental',
    unique_key='txn_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "txn_date_key",
      "data_type": "date",
      "granularity": "day"
    }
) }}

with silver_data as (
    select * from {{ ref('int_transactions_refined') }}
    where is_deleted = false
    
    {% if is_incremental() %}
      -- Gunakan _processed_at_gold (dari macro gold kau) untuk watermark
      and _processed_at > (select max(_processed_at_gold) from {{ this }})
    {% endif %}
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
        -- Panggil macro dengan argument yang betul
        {{ audit_columns(layer='gold') }}
    from silver_data
    where amount > 0 
      and cust_id is not null
)

select * from final_fact