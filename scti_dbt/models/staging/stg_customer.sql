with source as (

    select *
    from {{ ref('scd_raw_customer') }}
    where dbt_valid_to is null
      and coalesce(is_deleted, false) = false

),

renamed as (

    select
        customer_id::number as customer_id,

        trim(first_name)::varchar as first_name,
        trim(last_name)::varchar as last_name,
        lower(trim(email))::varchar as email,

        date_of_birth::date as date_of_birth,
        upper(trim(country))::varchar as country,

        created_at::timestamp_ntz as customer_created_at,
        updated_at::timestamp_ntz as customer_updated_at,

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