{{ config(
    alias='gold_data',
    materialized='table'
        ) 
}}

with gold_data as (

    select * from data_staging.silver_clean_data

)

select *
from gold_data
