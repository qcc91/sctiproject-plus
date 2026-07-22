with source as (

    select *
    from {{ source('raw', 'payment') }}
    where coalesce(is_deleted, false) = false

),

renamed as (

    select
        payment_id::number as payment_id,
        policy_id::number as policy_id,

        lower(trim(payment_status))::varchar as payment_status,
        lower(trim(payment_method))::varchar as payment_method,

        amount::number(18,2) as payment_amount,
        upper(trim(currency))::varchar as currency,
        payment_date::date as payment_date,

        pk_id,
        source_system,
        source_table,
        source_pk,
        batch_id,
        ingestion_job,
        load_date,
        load_timestamp,
        record_hash,
        is_deleted

    from source

)

select *
from renamed