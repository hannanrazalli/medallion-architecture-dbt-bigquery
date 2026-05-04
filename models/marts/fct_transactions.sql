{%- set cfg = var('fact_tables')['fct_transactions'] -%}
{%- set source_silver = ref(cfg['source']) -%}
{%- set fact_cols = cfg['columns'] -%}

{{ config(
    materialized='incremental',
    unique_key='txn_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

SELECT
    {% for cols in fact_cols %}
        {{ cols }}{% if not loop.last %}, {% endif %}
    {% endfor %},
    CAST(txn_date AS DATE) AS txn_date_key,
    _processed_at
FROM {{ source_silver }}