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
    from {{ source('silver_schema', 'customers_silver_layer') }}
    where is_current = TRUE

),

sales as (
    select * from {{ ref('aggregated_sales_transactions') }}
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