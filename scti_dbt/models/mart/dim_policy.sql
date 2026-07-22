{{ config(materialized='table') }}

select
    policy_id,
    customer_id,
    policy_number,
    product_id,
    policy_status,
    issue_date,
    policy_start_date,
    policy_end_date,
    coverage_days,
    is_currently_active,
    currency,
    sales_channel,
    load_timestamp,
    current_timestamp() as dbt_updated_at
from {{ ref('int_policy') }}