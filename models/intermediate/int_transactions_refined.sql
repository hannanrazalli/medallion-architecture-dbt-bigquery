{{ config(
    materialized='incremental',
    unique_key='txn_id',
    incremental_strategy='merge'
) }}

with source_data as (
    select * from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
      where _ingest_at > (select timestamp_sub(max(_ingest_at), interval 1 hour) from {{ this }})
    {% endif %}
),

deduplicated as (
    select *
    from source_data
    qualify row_number() over (
        partition by txn_id 
        order by _ingest_at desc
    ) = 1
),

transformed as (
    select
        txn_id,
        cust_id,
        cast(amount as float64) as amount,
        cast(points as int64) as points,
        is_member,
        case 
            when upper(trim(status)) in ('COMPLETED', 'PENDING', 'CANCELLED') 
            then upper(trim(status))
            else 'UNKNOWN'
        end as status,
        (upper(trim(status)) = 'CANCELLED') as is_deleted,
        txn_date,
        _ingest_at,
        _batch_id_bronze,
        _source_file,
        _record_status, 
        {{ audit_columns('silver') }}
    from deduplicated
)

select * from transformed
where amount is not null 
  and points is not null
  and _record_status = 'CLEAN'