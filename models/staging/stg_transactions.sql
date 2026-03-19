{{ config(
    materialized='incremental', 
    unique_key='txn_id'
) }}

with raw_data as (
    select * from {{ source('transactions_source', 'transactions_batch_1') }}
)

select
    txn_id,
    cust_id,
    amount,
    points,
    is_member,
    status,
    txn_date,

    case
        when txn_id is null or 