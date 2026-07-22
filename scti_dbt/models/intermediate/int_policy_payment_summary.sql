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

    from {{ ref('int_payment') }}

    {% if interval_start and interval_end %}

        -- Airflow 正常调度或 Backfill：
        -- 找出指定业务时间区间内发生付款变化的保单
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
        -- 从目标汇总表的当前业务时间水位继续处理
        where load_timestamp >= (
            select coalesce(
                max(load_timestamp),
                '1900-01-01'::timestamp_ntz
            )
            from {{ this }}
        )

    {% endif %}

),

payment as (

    select
        p.*

    from {{ ref('int_payment') }} as p

    {% if is_incremental() %}

        -- 只重新计算受影响的保单，
        -- 但读取这些保单现有的全部 payment
        inner join changed_policies as cp
            on p.policy_id = cp.policy_id

    {% endif %}

),

summary as (

    select
        policy_id,

        count(*) as payment_count,

        count_if(payment_status = 'paid')
            as paid_payment_count,

        count_if(payment_status = 'refunded')
            as refunded_payment_count,

        count_if(payment_status = 'failed')
            as failed_payment_count,

        count_if(payment_status = 'pending')
            as pending_payment_count,

        sum(
            case
                when payment_status = 'paid'
                    then payment_amount
                else 0
            end
        ) as paid_amount,

        sum(
            case
                when payment_status = 'refunded'
                    then payment_amount
                else 0
            end
        ) as refunded_amount,

        sum(
            case
                when payment_status = 'failed'
                    then payment_amount
                else 0
            end
        ) as failed_amount,

        sum(
            case
                when payment_status = 'pending'
                    then payment_amount
                else 0
            end
        ) as pending_amount,

        min(payment_date) as first_payment_date,
        max(payment_date) as latest_payment_date,

        max(load_timestamp) as load_timestamp,

        current_timestamp() as dbt_updated_at

    from payment

    group by policy_id

)

select *
from summary