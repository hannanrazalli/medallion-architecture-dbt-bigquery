{%- set source_model = ref('int_transactions_refined') -%}

{%- set columns = adapter.get_columns_in_relation(source_model) -%}
{%- set pk = "cust_id" -%} {#-- Kau cuma perlu define PK kat sini je --#}

{{ config(
    materialized='incremental',
    unique_key='hash_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with source_data as (
    select
        {% for col in columns -%}
            {{ col.name }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        _processed_at as valid_from
    from {{ source_model }}
    {% if is_incremental() %}
      where _processed_at > (select max(valid_from) from {{ this }})
    {% endif %}
),

deduplicated as (
    select *
    from source_data
    qualify row_number() over (
        partition by {{ pk }} 
        order by valid_from desc
    ) = 1
),

final_staged as (
    select
        {{ dbt_utils.generate_surrogate_key(columns | map(attribute='name') | list) }} as hash_key,
        {% for col in columns -%}
            {{ col.name }},
        {%- endfor %}
        
        valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current,
        {{ audit_columns(layer='gold') }}
    from deduplicated
)

select * from final_staged

{% if is_incremental() %}
union all

select
    t.hash_key,
    {% for col in columns -%}
        t.{{ col.name }},
    {%- endfor %}
    t.valid_from,
    s.valid_from as valid_to,
    false as is_current,
    {{ audit_columns(layer='gold') }}
from {{ this }} t
inner join final_staged s 
    on t.{{ pk }} = s.{{ pk }}
where t.is_current = true
  and t.hash_key <> s.hash_key
{% endif %}