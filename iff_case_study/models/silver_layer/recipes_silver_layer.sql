{{ config(
    alias='recipes_silver_layer',
    materialized='table',
    tags = 'silver_layer'
        ) 
}}

with silver_layer as (

    select * 
    from {{ source('silver_schema', 'recipes_pre_silver_layer') }}
)

select *
from silver_layer
