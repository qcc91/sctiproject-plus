{{ config(materialized='table') }}

with policy as (

    select *
    from {{ ref('int_policy') }}

),

customer as (

    select *
    from {{ ref('int_customer') }}

),

claim_summary as (

    select *
    from {{ ref('int_policy_claim_summary') }}

),

payment_summary as (

    select *
    from {{ ref('int_policy_payment_summary') }}

)

select
    p.policy_id,
    p.policy_number,
    p.policy_status,
    p.product_id,
    p.customer_id,

    c.country as customer_country,
    c.age as customer_age,
    c.age_group,

    p.issue_date,
    p.policy_start_date,
    p.policy_end_date,
    p.coverage_days,
    p.is_currently_active,

    p.premium_amount,
    p.currency,
    p.sales_channel,

    -------------------------
    -- Claim Summary
    -------------------------

    coalesce(cl.total_claim_count, 0) as total_claim_count,
    coalesce(cl.approved_claim_count, 0) as approved_claim_count,
    coalesce(cl.pending_claim_count, 0) as pending_claim_count,
    coalesce(cl.rejected_claim_count, 0) as rejected_claim_count,

    coalesce(cl.total_claim_amount, 0) as total_claim_amount,
    coalesce(cl.total_approved_amount, 0) as total_approved_amount,

    cl.latest_claim_date,

    -------------------------
    -- Payment Summary
    -------------------------

    coalesce(pay.payment_count, 0) as payment_count,

    coalesce(pay.paid_amount, 0) as paid_amount,
    coalesce(pay.refunded_amount, 0) as refunded_amount,
    coalesce(pay.failed_amount, 0) as failed_amount,
    coalesce(pay.pending_amount, 0) as pending_amount,

    pay.latest_payment_date,

    -------------------------
    -- Business Metrics
    -------------------------

    coalesce(cl.total_approved_amount, 0)
        / nullif(p.premium_amount, 0)
        as loss_ratio,

    case
        when coalesce(cl.total_approved_amount, 0)
             > p.premium_amount
        then true
        else false
    end as is_high_loss_policy,

    greatest(
        p.load_timestamp,
        c.load_timestamp,
        coalesce(cl.load_timestamp, p.load_timestamp),
        coalesce(pay.load_timestamp, p.load_timestamp)
    ) as load_timestamp,

    current_timestamp() as dbt_updated_at

from policy p

left join customer c
    on p.customer_id = c.customer_id

left join claim_summary cl
    on p.policy_id = cl.policy_id

left join payment_summary pay
    on p.policy_id = pay.policy_id