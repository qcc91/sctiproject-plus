{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

{% set interval_start = env_var('DBT_DATA_INTERVAL_START', '') %}
{% set interval_end = env_var('DBT_DATA_INTERVAL_END', '') %}

with payment as (

    select *
    from {{ ref('stg_payment') }}

    {% if interval_start and interval_end %}

        -- Airflow 正常调度或 Backfill：处理指定时间区间
        where load_timestamp >= convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_start }}')
        )::timestamp_ntz

          and load_timestamp < convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_end }}')
        )::timestamp_ntz

    {% elif is_incremental() %}

        -- 本地手动运行：继续使用目标表最大水位
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
        payment_id,
        policy_id,

        payment_status,
        payment_method,
        payment_amount,
        currency,
        payment_date,

        case
            when payment_status = 'paid' then true
            else false
        end as is_successful_payment,

        case
            when payment_status = 'refunded' then true
            else false
        end as is_refunded,

        source_system,
        source_table,
        source_pk,

        batch_id,
        ingestion_job,

        load_date,
        load_timestamp,

        current_timestamp() as dbt_updated_at

    from payment

)

select *
from transformed