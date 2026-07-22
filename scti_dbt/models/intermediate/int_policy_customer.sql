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

),

customer as (

    select *
    from {{ ref('stg_customer') }}

),

changed_policy_ids as (

    /*
      情况一：policy 自身在本次业务时间区间内发生变化
    */

    select
        p.policy_id

    from policy as p

    {% if interval_start and interval_end %}

        where p.load_timestamp >= convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_start }}')
        )::timestamp_ntz

          and p.load_timestamp < convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_end }}')
        )::timestamp_ntz

    {% elif is_incremental() %}

        where p.load_timestamp >= (
            select coalesce(
                max(policy_load_timestamp),
                '1900-01-01'::timestamp_ntz
            )
            from {{ this }}
        )

    {% endif %}

    union

    /*
      情况二：customer 在本次业务时间区间内发生变化，
      所有关联该 customer 的 policy 都需要重新计算
    */

    select
        p.policy_id

    from policy as p

    inner join customer as c
        on p.customer_id = c.customer_id

    {% if interval_start and interval_end %}

        where c.load_timestamp >= convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_start }}')
        )::timestamp_ntz

          and c.load_timestamp < convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_end }}')
        )::timestamp_ntz

    {% elif is_incremental() %}

        where c.load_timestamp >= (
            select coalesce(
                max(customer_load_timestamp),
                '1900-01-01'::timestamp_ntz
            )
            from {{ this }}
        )

    {% endif %}

),

selected_policy as (

    select
        p.*

    from policy as p

    {% if is_incremental() %}

        inner join changed_policy_ids as cp
            on p.policy_id = cp.policy_id

    {% endif %}

),

joined_source as (

    select
        p.policy_id,
        p.policy_number,
        p.policy_status,
        p.product_id,

        p.customer_id,
        c.country as customer_country,
        c.date_of_birth,

        p.issue_date,
        p.policy_start_date,
        p.policy_end_date,

        datediff(
            day,
            p.policy_start_date,
            p.policy_end_date
        ) + 1 as policy_duration_days,

        p.premium_amount,
        p.currency,
        p.sales_channel,

        p.source_system,
        p.batch_id,
        p.ingestion_job,
        p.load_date,

        p.load_timestamp as policy_load_timestamp,
        c.load_timestamp as customer_load_timestamp,

        greatest(
            p.load_timestamp,
            coalesce(c.load_timestamp, p.load_timestamp)
        ) as load_timestamp

    from selected_policy as p

    left join customer as c
        on p.customer_id = c.customer_id

),

final as (

    select
        policy_id,
        policy_number,
        policy_status,
        product_id,

        customer_id,
        customer_country,
        date_of_birth,

        /*
          按该记录最新业务时间计算年龄，
          而不是按照 dbt 实际运行当天计算。
        */
        datediff(
            year,
            date_of_birth,
            load_timestamp::date
        ) as customer_age,

        issue_date,
        policy_start_date,
        policy_end_date,
        policy_duration_days,

        premium_amount,
        currency,
        sales_channel,

        source_system,
        batch_id,
        ingestion_job,
        load_date,

        policy_load_timestamp,
        customer_load_timestamp,
        load_timestamp,

        current_timestamp() as dbt_updated_at

    from joined_source

)

select *
from final