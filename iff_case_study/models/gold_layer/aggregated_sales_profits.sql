{{ config(
    materialized='view',
    tags = 'gold_layer'
        ) 
}}


with sales as (
    select * from {{ ref('sales_transactions_with_costs') }}
)

select
    flavour_id,
    customer_id,
    country,
    town,
    postal_code,
    date_trunc('year', transaction_date) as year,
    date_trunc('quarter', transaction_date) as quarter,
    date_trunc('month', transaction_date) as month,
    date_trunc('week', transaction_date) as week,
    sum(amount_dollar) as total_sales,
    sum(total_cost) as total_cost,
    sum(profit) as total_profit
from sales
group by flavour_id, customer_id, country, town, postal_code, year, quarter, month, week