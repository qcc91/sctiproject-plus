with source as (

    select *
    from {{ source('raw', 'claim') }}
    where coalesce(is_deleted, false) = false

),

renamed as (

    select
        claim_id::number as claim_id,
        policy_id::number as policy_id,

        upper(trim(claim_number))::varchar as claim_number,
        lower(trim(claim_type))::varchar as claim_type,
        lower(trim(claim_status))::varchar as claim_status,

        claim_date::date as claim_date,
        claim_amount::number(18,2) as claim_amount,
        approved_amount::number(18,2) as approved_amount,

        created_at::timestamp_ntz as claim_created_at,
        updated_at::timestamp_ntz as claim_updated_at,

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