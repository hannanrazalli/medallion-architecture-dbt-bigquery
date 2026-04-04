{%- set cfg = var('fact_tables')['fct_transactions'] -%}
{%- set source_model = ref(cfg['source']) -%}
{%- set fact_cols = cfg['columns'] -%}
{%- set pk = fact_cols[0] -%}

{{ config(
    materialized='incremental',
    unique_key=pk,
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
        {% for col in fact_cols -%}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        cast(txn_date as date) as txn_date_key,
        _processed_at 
    from {{ source_model }}
    where _is_deleted = false
    
    {% if is_incremental() %}
      and _processed_at > (select max(_processed_at_gold) from {{ this }})
    {% endif %}
),

final_fact as (
    select
        {{ dbt_utils.generate_surrogate_key(fact_cols) }} as fact_hash_key,
        {% for col in fact_cols -%}
            {{ col }},
        {%- endfor %}
        txn_date_key,
        {{ audit_columns('gold') }}
    from silver_data
    where amount > 0 
      and cust_id is not null
)

select * from final_fact