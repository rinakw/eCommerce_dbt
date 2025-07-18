{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

select distinct(CUSTOMERID) as customer_id
from 
{{ source('orders', 'orders') }}
where CUSTOMERID is not null