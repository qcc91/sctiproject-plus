{{
    config(
        materialized='table',
        alias='fct_claim'
    )
}}

select
    claim_id,
    policy_id,
    claim_number,
    claim_type,
    claim_status,
    claim_date,
    claim_amount,
    approved_amount,
    approval_ratio,
    fully_paid,
    has_payment,
    claim_created_at,
    claim_updated_at,
    load_timestamp,
    current_timestamp() as dbt_updated_at

from {{ ref('int_claim') }}