name: 'practice1_medallion'
version: '1.0.0'
config-version: 2
profile: 'default'

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

models:
  practice1_medallion:
    staging:
      +schema: bronze
      +materialized: incremental
    
    intermediate:
      +schema: silver
      +materialized: incremental

    marts:
      +schema: gold
      +materialized: incremental

vars:
  dim_tables:
    dim_customers:
      source: "int_transactions_refined"
      columns: ["cust_id", "is_member"]