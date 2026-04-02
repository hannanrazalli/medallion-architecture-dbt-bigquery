{#-- PHASE 3: DIM ENGINE (dbt version) --#}
{%- set source_model = ref('int_transactions_refined') -%}

{#-- Sini kau letak column yang kau nak sahaja (macam config dlm PySpark) --#}
{%- set dim_cols = ['cust_id', 'is_member'] -%}
{%- set pk = dim_cols[0] -%} {#-- Ambil cust_id sebagai PK --#}

{{ config(
    materialized='incremental',
    unique_key='hash_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with source_data as (
    select
        -- Hanya loop column yang kita dah define dalam dim_cols
        {% for col in dim_cols -%}
            {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        _processed_at as valid_from
    from {{ source_model }}
    {% if is_incremental() %}
      -- Watermark: Jimat kos BQ
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
        -- Generate hash_key guna list dim_cols sahaja
        {{ dbt_utils.generate_surrogate_key(dim_cols) }} as hash_key,
        
        {% for col in dim_cols -%}
            {{ col }},
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

-- PRO LOGIC: Expire-kan record lama
select
    t.hash_key,
    {% for col in dim_cols -%}
        t.{{ col }},
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