{{ config(
    materialized='view',
    tags = 'gold_layer'
        ) 
}}
with sales_yoy as (
    select
        flavour_id,
        customer_id,
        country,
        year,
        sum(total_amount_dollar) as total_sales
    from {{ ref('customer_sales') }}
    group by flavour_id, customer_id, country, year
),

sales_comparison as (
    select
        s1.flavour_id,
        s1.customer_id,
        s1.country,
        s1.year as year1,
        s1.total_sales as sales1,
        s2.year as year2,
        s2.total_sales as sales2,
        (s2.total_sales - s1.total_sales) as sales_difference,
        case
            when s2.total_sales > s1.total_sales then 'Improved'
            else 'Underperformed'
        end as performance
    from sales_yoy s1
    join sales_yoy s2
    on s1.flavour_id = s2.flavour_id
    and s1.customer_id = s2.customer_id
    and s1.country = s2.country
    and s1.year = s2.year - interval '1 year'
)

select * from sales_comparison