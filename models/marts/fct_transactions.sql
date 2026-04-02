{#-- PHASE 4: FACT ENGINE (dbt version) --#}
{%- set source_model = ref('int_transactions_refined') -%}

{#-- Sini kau letak column yang kau nak sahaja (Sebiji macam dlm Databricks kau) --#}
{%- set fact_cols = ['txn_id', 'cust_id', 'amount', 'points', 'status', 'txn_date'] -%}

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
    select 
        -- 1. Hanya select column yang kita nak dari list fact_cols
        {% for col in fact_cols -%}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        -- 2. Tambah partition key (Business Date)
        cast(txn_date as date) as txn_date_key,
        -- Tambah _processed_at supaya kita boleh buat watermark (tapi tak masuk final select kalau tak nak)
        _processed_at 
    from {{ source_model }}
    where is_deleted = false
    
    {% if is_incremental() %}
      -- Watermark logic: Bandingkan dengan _processed_at_gold dalam table Gold sedia ada
      and _processed_at > (select max(_processed_at_gold) from {{ this }})
    {% endif %}
),

final_fact as (
    select
        -- 3. Select semua dari silver_data (yang dah ditapis columns dia)
        {% for col in fact_cols -%}
            {{ col }},
        {%- endfor %}
        txn_date_key,
    from silver_data
    where amount > 0 
      and cust_id is not null
)

select * from final_fact
