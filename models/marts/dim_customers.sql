{{ config(
    materialized='incremental',
    unique_key='hash_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

-- CTE 1: Ambil data baru dari Silver (Incremental logic)
with source_data as (
    select
        cust_id,
        is_member,
        _processed_at as valid_from
    from {{ ref('int_transactions_refined') }}
    {% if is_incremental() %}
      -- Watermark logic untuk jimat kos BigQuery
      where _processed_at > (select max(valid_from) from {{ this }})
    {% endif %}
),

-- CTE 2: Deduplikasi (Grain of Truth - 1 row per customer per batch)
deduplicated as (
    select *
    from source_data
    qualify row_number() over (
        partition by cust_id 
        order by valid_from desc
    ) = 1
),

-- CTE 3: Prepare record baru (SCD 2 - New/Current Record)
final_staged as (
    select
        {{ dbt_utils.generate_surrogate_key(['cust_id', 'is_member']) }} as hash_key,
        cust_id,
        is_member,
        valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current,
        -- Panggil macro dengan string 'gold'
        {{ audit_columns('gold') }}
    from deduplicated
)

-- FINAL OUTPUT
select * from final_staged

{% if is_incremental() %}
union all

-- PRO LOGIC: "Expire-kan" record lama kalau ada perubahan hash_key
select
    t.hash_key,
    t.cust_id,
    t.is_member,
    t.valid_from,
    s.valid_from as valid_to, 
    false as is_current,
    -- Pastikan panggil macro yang sama untuk struktur column yang sama
    {{ audit_columns('gold') }}
from {{ this }} t
inner join final_staged s 
    on t.cust_id = s.cust_id
where t.is_current = true
  and t.hash_key <> s.hash_key 
{% endif %}