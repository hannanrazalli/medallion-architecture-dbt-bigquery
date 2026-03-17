{{ config(
    materialized='table',
    unique_key='cust_id',
    schema='gold'
) }}

with customer_source as (
    -- Ambil data dari Silver (Intermediate)
    select
        cust_id,
        is_member,
        _processed_at as valid_from
    from {{ ref('int_transactions_refined') }}
),

deduplicated as (
    -- Guna Window Function untuk ambil record paling latest bagi setiap customer
    -- Ini sebiji macam logic .dropDuplicates([pk]) dalam Spark kau
    select
        *,
        row_number() over (
            partition by cust_id 
            order by valid_from desc
        ) as rn
    from customer_source
),

final_dim as (
    select
        -- 1. Surrogate Key (Guna package dbt_utils yang kau install tadi)
        {{ dbt_utils.generate_surrogate_key(['cust_id', 'is_member']) }} as hash_key,
        
        -- 2. Business Columns
        cust_id,
        is_member,
        
        -- 3. SCD Tracking Columns (Macam dalam Spark Engine kau)
        valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current,
        
        -- 4. Audit Columns (Guna Macro universal yang kita buat tadi)
        {{ audit_columns('gold') }}
    from deduplicated
    where rn = 1
)

select * from final_dim