{%- set cfg = var('fact_tables')['fct_transactions'] -%}
{%- set source_silver = ref(cfg['source']) -%}
{%- set fact_cols = cfg['columns'] -%}
{%- set pk = fact_cols[0] -%}

{{ config(
    materialized='incremental',
    unique_key='hash_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    partition_by={
        "field" : "txn_date_key",
        "data_type" : "date",
        "granularity" : "day"
    }
) }}

WITH source_data AS (
    SELECT
        {% for cols in fact_cols %}
            {{ cols }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        _processed_at,
        cast(txn_date AS date) AS txn_date_key
    FROM {{ source_silver }}
    WHERE _is_deleted = false
    {% if is_incremental() %}
        AND _processed_at > (
            SELECT timestamp_sub(max(_processed_at), INTERVAL 1 HOUR)
            FROM {{ this }})
    {% endif %}
),

final_staged AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(fact_cols) }} AS hash_key,
        {% for cols in fact_cols %}
            {% if cols != 'txn_date' %}
                {{ cols }},
            {% endif %}
        {% endfor %}
        txn_date_key,
        {{ audit_columns('gold') }}
    FROM source_data
)

SELECT *
FROM final_staged