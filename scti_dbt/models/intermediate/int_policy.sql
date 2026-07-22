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

with policy as (

    select *
    from {{ ref('stg_policy') }}

    {% if interval_start and interval_end %}

        -- Airflow 正常调度或 Backfill：
        -- 处理指定业务时间区间内的当前 policy 记录
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
        -- 从目标表的最大业务时间戳继续处理
        where load_timestamp >= (
            select coalesce(
                max(load_timestamp),
                '1900-01-01'::timestamp_ntz
            )
            from {{ this }}
        )

    {% endif %}

),

transformed as (

    select
        policy_id,
        customer_id,

        policy_number,
        product_id,
        policy_status,

        issue_date,
        policy_start_date,
        policy_end_date,

        datediff(
            day,
            policy_start_date,
            policy_end_date
        ) + 1 as coverage_days,

        case
            when load_timestamp >= policy_start_date
             and load_timestamp < dateadd(
                    day,
                    1,
                    policy_end_date
                 )
                then true
            else false
        end as is_currently_active,

        premium_amount,
        currency,
        sales_channel,

        source_system,
        source_table,
        source_pk,

        batch_id,
        ingestion_job,

        load_date,
        load_timestamp,

        current_timestamp() as dbt_updated_at

    from policy

)

select *
from transformed