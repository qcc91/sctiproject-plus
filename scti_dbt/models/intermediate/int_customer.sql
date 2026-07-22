{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

{% set interval_start = env_var('DBT_DATA_INTERVAL_START', '') %}
{% set interval_end = env_var('DBT_DATA_INTERVAL_END', '') %}

with customer as (

    select *
    from {{ ref('stg_customer') }}

    {% if interval_start and interval_end %}

        -- Airflow 调度 / Backfill
        where load_timestamp >= convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_start }}')
        )::timestamp_ntz

        and load_timestamp < convert_timezone(
            'UTC',
            to_timestamp_tz('{{ interval_end }}')
        )::timestamp_ntz

    {% elif is_incremental() %}

        -- 本地手动执行
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

        customer_id,

        first_name,
        last_name,

        concat(first_name, ' ', last_name) as full_name,

        email,

        date_of_birth,

        datediff(year, date_of_birth, current_date()) as age,

        case
            when datediff(year, date_of_birth, current_date()) < 18 then 'Minor'
            when datediff(year, date_of_birth, current_date()) between 18 and 30 then 'Young Adult'
            when datediff(year, date_of_birth, current_date()) between 31 and 60 then 'Adult'
            else 'Senior'
        end as age_group,

        country,

        customer_created_at,
        customer_updated_at,

        source_system,
        source_table,
        source_pk,

        batch_id,
        ingestion_job,

        load_date,
        load_timestamp,

        current_timestamp() as dbt_updated_at

    from customer

)

select *
from transformed