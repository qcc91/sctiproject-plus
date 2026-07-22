{{ config(materialized='table') }}

select
    customer_id,
    full_name,
    country,
    age,
    age_group,
    customer_created_at,
    customer_updated_at,
    load_timestamp,
    current_timestamp() as dbt_updated_at
from {{ ref('int_customer') }}