{{
    config(
        materialized='incremental',
        unique_key='claim_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

{% set interval_start = env_var('DBT_DATA_INTERVAL_START', '') %}
{% set interval_end = env_var('DBT_DATA_INTERVAL_END', '') %}

with claim as (

    select *
    from {{ ref('stg_claim') }}

    {% if interval_start and interval_end %}

        -- Airflow 调度或 Backfill：处理指定的数据时间区间
        where load_timestamp >= convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_start }}')
        )::timestamp_ntz

          and load_timestamp < convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_end }}')
        )::timestamp_ntz

    {% elif is_incremental() %}

        -- 本地手动运行：继续使用目标表中的最大水位
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
        claim_id,
        policy_id,

        claim_number,
        claim_type,
        claim_status,

        claim_date,
        claim_amount,
        approved_amount,

        approved_amount
            / nullif(claim_amount, 0)
            as approval_ratio,

        case
            when approved_amount = claim_amount then true
            else false
        end as fully_paid,

        case
            when approved_amount > 0 then true
            else false
        end as has_payment,

        claim_created_at,
        claim_updated_at,

        source_system,
        source_table,
        source_pk,

        batch_id,
        ingestion_job,

        load_date,
        load_timestamp,

        current_timestamp() as dbt_updated_at

    from claim

)

select *
from transformed