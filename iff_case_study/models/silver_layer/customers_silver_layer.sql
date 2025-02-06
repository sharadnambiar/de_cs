{{ config(
    alias='customers_silver_layer',
    materialized='incremental',
    unique_key = 'customer_id',
    tags = 'silver_layer'
        ) 
}}


with source as (
    select
        customer_id,
        name,
        location_city,
        location_country,
        load_date,
        generation_date,
        batch_number,
        source_file,
        CURRENT_TIMESTAMP AS valid_from
    from {{ source('silver_schema', 'customers_pre_silver_layer') }}
)

{% if is_incremental() %}
,
existing_customers as (
    select
        customer_id,
        name,
        location_city,
        location_country,
        load_date,
        generation_date,
        batch_number,
        source_file,
        CURRENT_TIMESTAMP AS valid_from,
        valid_to,
        is_current
    from {{ this }}
    where is_current = TRUE
),

expired_customers as (
    select
        e.customer_id,
        e.name,
        e.location_city,
        e.location_country,
        e.load_date,
        e.generation_date,
        e.batch_number,
        e.source_file,
        e.valid_from,
        current_timestamp as valid_to,
        FALSE as is_current
    from existing_customers e
    join source s
    on e.customer_id = s.customer_id
    where e.name != s.name
    or e.location_city != s.location_city
    or e.location_country != s.location_country
),

new_customers as (
    select
        s.customer_id,
        s.name,
        s.location_city,
        s.location_country,
        s.load_date,
        s.generation_date,
        s.batch_number,
        s.source_file,
        current_timestamp as valid_from,
        null::timestamp as valid_to,
        TRUE as is_current
    from source s
    left join existing_customers e
    on s.customer_id = e.customer_id
    where e.customer_id is null
    or e.name != s.name
    or e.location_city != s.location_city
    or e.location_country != s.location_country
)

select * from existing_customers
union all
select * from expired_customers
union all
select * from new_customers

{% else %}

select
    customer_id,
    name,
    location_city,
    location_country,
    load_date,
    generation_date,
    batch_number,
    source_file,
    current_timestamp as valid_from,
    null::timestamp as valid_to,
    true as is_current
from source

{% endif %}
