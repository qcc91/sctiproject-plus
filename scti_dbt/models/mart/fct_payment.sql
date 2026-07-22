{{
    config(
        materialized='table'
    )
}}

select
    payment_id,
    policy_id,
    payment_status,
    payment_method,
    payment_amount,
    currency,
    payment_date,
    is_successful_payment,
    is_refunded,
    load_timestamp,
    current_timestamp() as dbt_updated_at

from {{ ref('int_payment') }}