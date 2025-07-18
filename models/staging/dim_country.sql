{{
    config(
        materialized='incremental',
        unique_key='country_name'
    )
}}

with country as (
    select 
        distinct(COUNTRY) as country_name
    from 
        {{ source('orders', 'orders') }}
    where 
        COUNTRY is not null 
     or COUNTRY <> 'Unspecified'
),

ref as(
    select * from {{ ref('country_list') }}
),

joined as (

    select 
        country_name, 
        r.iso_code,
        r.region, 
        r.sub_region, 
        r.latitude, 
        r.longitude
    from country c
    left join ref r 
        on lower(c.country_name) = lower(r.name)
)

select * from joined