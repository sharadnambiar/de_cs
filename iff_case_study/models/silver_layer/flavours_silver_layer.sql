{{ config(
    alias='flavours_silver_layer',
    materialized='table',
    tags = 'silver_layer'
        ) 
}}

with silver_data as (

    select * 
    from {{ source('silver_schema', 'flavours_pre_silver_layer') }}

)

select *
from silver_data
