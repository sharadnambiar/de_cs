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
    from {{ ref('salestransactions_silver_layer') }}
),

ingredients as (
    select
        ingredient_id,
        cost_per_gram
    from {{ ref('ingredients_silver_layer') }}
),

recipes as (
    select
        flavour_id,
        ingredient_id
    from {{ ref('recipes_silver_layer') }}
),

joined as (
    select
        s.transaction_id,
        s.customer_id,
        s.flavour_id,
        s.quantity_liters,
        s.transaction_date,
        s.country,
        s.town,
        s.postal_code,
        s.amount_dollar,
        i.cost_per_gram
    from sales s
    left join recipes r
    on s.flavour_id = r.flavour_id
    left join ingredients i
    on r.ingredient_id = i.ingredient_id
),

sales_with_costs as (
    select
        transaction_id,
        customer_id,
        flavour_id,
        quantity_liters,
        transaction_date,
        country,
        town,
        postal_code,
        amount_dollar,
        cost_per_gram * quantity_liters as total_cost,
        amount_dollar - (cost_per_gram * quantity_liters) as profit
    from joined
)

select * from sales_with_costs