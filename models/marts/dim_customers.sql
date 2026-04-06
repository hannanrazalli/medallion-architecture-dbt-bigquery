{%- set cfg = var('dim_tables')['dim_customers'] -%}
{%- set source_silver = ref(cfg['source']) -%}
{%- set dim_cols = cfg['columns'] -%}
{%- set pk = dim_cols[0] -%}

{{ config(
    materialized='incremental',
    unique_key='hash_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH source_data AS (
    SELECT
        {% for cols in dim_cols %}
            {{ cols }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        _processed_at AS valid_from
    FROM {{ source_silver }}
    {% if is_incremental() %}
        WHERE _processed_at > (
            SELECT max(valid_from)
            FROM {{ this }})
    {% endif %}
),

deduplicate AS (
    SELECT *
    FROM source_data
    qualify row_number() over(
        partition by {{ pk }}
        order by valid_from desc
    ) = 1
),

final_staged AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(dim_cols) }} AS hash_key,
        {% for cols in dim_cols %}
            {{ cols }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        valid_from,
        cast(null as timestamp) AS valid_to,
        true AS is_current,
        {{ audit_columns('gold') }}
    FROM deduplicate
)

SELECT *
FROM final_staged

{% if is_incremental() %}
union all

SELECT
    t.hash_key,
    {% for cols in dim_cols %}
        t.{{ cols }}{% if not loop.last %}, {% endif %}
    {% endfor %},
    t.valid_from,
    s.valid_from AS valid_to,
    false AS is_current,
    {{ audit_columns }}
FROM {{ this }} t
INNER JOIN final_staged s
    ON t.{{ pk }} = s.{{ pk }}
WHERE t.is_current = true
    AND t.hash_key <> s.hash_key

{% endif %}