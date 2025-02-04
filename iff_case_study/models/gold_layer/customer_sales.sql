{{ config(
    materialized='view',
    tags = 'gold_layer'
        ) 
}}
with customers as (
    select
        customer_id,
        name,
        location_city,
        location_country
    from {{ ref('customers_silver_layer') }}
),

sales as (
    select * from {{ ref('sales_transactions_agg') }}
)

select
    c.customer_id,
    c.name,
    c.location_city,
    c.location_country,
    s.flavour_id,
    s.total_quantity_liters,
    s.total_amount_dollar,
    s.year,
    s.country,
    s.town,
    s.postal_code
from customers c
left join sales s
on c.customer_id = s.customer_id