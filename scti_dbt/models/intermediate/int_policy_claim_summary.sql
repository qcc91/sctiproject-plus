{{
    config(
        materialized='incremental',
        unique_key='policy_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

{% set interval_start = env_var('DBT_DATA_INTERVAL_START', '') %}
{% set interval_end = env_var('DBT_DATA_INTERVAL_END', '') %}

with changed_policies as (

    select distinct
        policy_id

    from {{ ref('int_claim') }}

    {% if interval_start and interval_end %}

        -- Airflow 正常调度或 Backfill：
        -- 找出本次时间区间内存在 claim 变化的 policy
        where load_timestamp >= convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_start }}')
        )::timestamp_ntz

          and load_timestamp < convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_end }}')
        )::timestamp_ntz

    {% elif is_incremental() %}

        -- 本地手动增量运行：
        -- 根据本汇总表当前最大水位识别受影响的 policy
        where load_timestamp >= (
            select coalesce(
                max(load_timestamp),
                '1900-01-01'::timestamp_ntz
            )
            from {{ this }}
        )

    {% endif %}

),

claim as (

    select
        c.*

    from {{ ref('int_claim') }} as c

    {% if is_incremental() %}

        -- 只重新计算受影响的 policy，
        -- 但读取这些 policy 当前保留的全部 claim
        inner join changed_policies as cp
            on c.policy_id = cp.policy_id

    {% endif %}

),

summary as (

    select
        policy_id,

        count(*) as total_claim_count,

        count_if(claim_status = 'approved')
            as approved_claim_count,

        count_if(claim_status = 'pending')
            as pending_claim_count,

        count_if(claim_status = 'rejected')
            as rejected_claim_count,

        count_if(claim_status = 'under review')
            as under_review_claim_count,

        count_if(fully_paid)
            as fully_paid_claim_count,

        count_if(has_payment)
            as claims_with_payment_count,

        sum(claim_amount)
            as total_claim_amount,

        sum(coalesce(approved_amount, 0))
            as total_approved_amount,

        sum(coalesce(approved_amount, 0))
            / nullif(sum(claim_amount), 0)
            as overall_approval_ratio,

        min(claim_date)
            as first_claim_date,

        max(claim_date)
            as latest_claim_date,

        max(load_timestamp)
            as load_timestamp,

        current_timestamp()
            as dbt_updated_at

    from claim

    group by policy_id

)

select *
from summary