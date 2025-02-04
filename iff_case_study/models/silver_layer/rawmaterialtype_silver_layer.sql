{{ config(
    alias='rawmaterialtype_silver_layer',
    materialized='table',
    tags = 'silver_layer'
        ) 
}}

with silver_layer as (

    select * from silver_schema.rawmaterialtype_pre_silver_layer
)

select *
from silver_layer
