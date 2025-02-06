{{ config(
    materialized='view',
    tags = 'gold_layer'
        ) 
}}

with sales as (
    select
        transaction_id,
        customer_id,
        flavour_id,
        quantity_liters,
        transaction_date,
        country,
        town,
        postal_code,
        amount_dollar
    from {{ source('silver_schema', 'salestransactions_silver_layer') }}
)

select
    customer_id,
    flavour_id,
    sum(quantity_liters) as total_quantity_liters,
    sum(amount_dollar) as total_amount_dollar,
    date_trunc('year', transaction_date) as year,
    country,
    town,
    postal_code
from sales
group by customer_id, flavour_id, year, country, town, postal_code