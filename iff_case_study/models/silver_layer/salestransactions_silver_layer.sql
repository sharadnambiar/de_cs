{{ config(
    alias='salestransactions_silver_layer',
    materialized='incremental',
    unique_key='transaction_id',
    tags = 'silver_layer'
        ) 
}}

with silver_layer as (

    select * from silver_schema.salestransactions_pre_silver_layer
)
select *
from silver_layer
