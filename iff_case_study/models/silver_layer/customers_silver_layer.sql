{{ config(
    alias='customers_silver_layer',
    materialized='incremental',
    unique_id = 'customer_id',
    tags = 'silver_layer'
        ) 
}}

with silver_layer as (

    select * from silver_schema.customers_pre_silver_layer
)

select *
from silver_layer

