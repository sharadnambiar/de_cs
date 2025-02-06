{{ config(
    materialized='view',
    tags = 'gold_layer'
        ) 
}}
with avg_consumption as (
    select
        flavour_id,
        avg(total_quantity_liters) as avg_quantity_liters
    from {{ ref('customer_sales') }}
    group by flavour_id
),

customer_consumption as (
    select
        cs.customer_id,
        cs.name,
        cs.flavour_id,
        cs.total_quantity_liters,
        ac.avg_quantity_liters,
        case
            when cs.total_quantity_liters > ac.avg_quantity_liters then 'Top Consumer'
            else 'Potential Client'
        end as consumption_category
    from {{ ref('customer_sales') }} cs
    join avg_consumption ac
    on cs.flavour_id = ac.flavour_id
)

select * from customer_consumption