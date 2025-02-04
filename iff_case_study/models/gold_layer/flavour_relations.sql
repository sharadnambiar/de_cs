{{ config(
    materialized='view',
    tags = 'gold_layer'
        ) 
}}

with flavour_pairs as (
    select
        st1.customer_id,
        st1.flavour_id as flavour_id1,
        st2.flavour_id as flavour_id2,
        st1.country,
        st1.town,
        st1.postal_code,
        count(*) as pair_count
    from {{ ref('salestransactions_silver_layer') }} st1
    join {{ ref('salestransactions_silver_layer') }} st2
    on st1.customer_id = st2.customer_id
    and st1.transaction_id != st2.transaction_id
    and st1.transaction_date = st2.transaction_date
    group by st1.customer_id, st1.flavour_id, st2.flavour_id, st1.country, st1.town, st1.postal_code
)

select
    flavour_id1,
    flavour_id2,
    sum(pair_count) as total_pair_count,
    count(distinct customer_id) as unique_customers,
    count(distinct country) as unique_countries,
    count(distinct town) as unique_towns,
    count(distinct postal_code) as unique_postal_codes
from flavour_pairs
group by flavour_id1, flavour_id2 order by total_pair_count desc