{{ config(
    materialized='incremental',
    unique_key='hash_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH source_data AS (
    SELECT
        cust_id,
        is_member,
        _processed_at AS valid_from
    FROM {{ ref('int_transactions_refined') }}
    {% if is_incremental() %}
      WHERE _processed_at > (
        SELECT max(valid_from)
        FROM {{ this }}
      )
    {% endif %}
),

deduplicate AS (
    SELECT *
    FROM source_data
    qualify row_number() over(
        partition by cust_id
        order by valid_from desc
    ) = 1
),

final_staged AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['cust_id', 'is_member']) }} AS hash_key,
        cust_id,
        is_member,
        valid_from,
        cast(NULL as timestamp) AS valid_to,
        true as is_current,
        {{ audit_columns('gold') }}
    FROM deduplicate
)

SELECT *
FROM final_staged

{% if is_incremental() %}
union all

SELECT
    t.hash_key,
    t.cust_id,
    t.is_member,
    t.valid_from,
    s.valid_from AS valid_to,
    false AS is_current,
    {{ audit_columns('gold') }}
FROM {{ this }} t
INNER JOIN final_staged s
    ON t.cust_id = s._cust_id
WHERE t.is_current = true
  AND t.hash_key <> s.hash_key
{% endif %}