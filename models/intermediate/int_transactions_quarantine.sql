{{ config(materialized='incremental', unique_key='txn_id') }}

with source as (
    select * from {{ ref('stg_transactions') }}
)

-- Ambil data yang memang CORRUPT dari Bronze
-- ATAU yang gagal QC di Silver (Amount/Points NULL)
select 
    *,
    current_timestamp() as _quarantined_at
from source
where _record_status = 'CORRUPT'
   or amount is null
   or points is null