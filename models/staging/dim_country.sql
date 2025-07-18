{{
    config(
        materialized='incremental',
        unique_key='country'
    )
}}

select distinct(COUNTRY) as country
from 
{{ source('orders', 'orders') }}
where COUNTRY is not null