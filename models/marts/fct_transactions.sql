{%- set source_model = ref('int_transactions_refined') -%}
{%- set columns = adapter.get_columns_in_relation(source_model) -%}

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
        {% for col in columns -%}
            {{ col.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        cast(txn_date as date) as txn_date_key
    from {{ source_model }}
    where is_deleted = false
    
    {% if is_incremental() %}
      and _processed_at > (select max(_processed_at_gold) from {{ this }})
    {% endif %}
),

final_fact as (
    select
        * ,
        {{ audit_columns(layer='gold') }}
    from silver_data
    where amount > 0 
      and cust_id is not null
)

select * from final_fact