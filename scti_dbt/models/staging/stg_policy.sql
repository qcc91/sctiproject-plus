with source as (

    select *
    from {{ ref('scd_raw_policy') }}
    where dbt_valid_to is null
      and coalesce(is_deleted, false) = false

),

renamed as (

    select
        policy_id::number as policy_id,
        customer_id::number as customer_id,

        upper(trim(product_id))::varchar as product_id,
        upper(trim(policy_number))::varchar as policy_number,
        lower(trim(policy_status))::varchar as policy_status,

        issue_date::date as issue_date,
        start_date::date as policy_start_date,
        end_date::date as policy_end_date,

        premium_amount::number(18,2) as premium_amount,
        upper(trim(currency))::varchar as currency,
        lower(trim(channel))::varchar as sales_channel,

        pk_id,
        source_system,
        source_table,
        source_pk,
        batch_id,
        ingestion_job,
        load_date,
        load_timestamp,
        record_hash,
        is_deleted,

        dbt_valid_from,
        dbt_valid_to,
        dbt_scd_id,
        dbt_updated_at

    from source

)

select *
from renamed