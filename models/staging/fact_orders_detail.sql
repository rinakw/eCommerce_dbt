{{
    config(
        materialized='incremental'
    )
}}
with 
base as (
    select 
        invoiceno , 
        stockcode ,
        invoicedate ,
        quantity, 
        unitprice,
        (quantity * unitprice) as trx_amt
    from 
    {{ source('orders', 'orders') }}
), 

ref_products as (
    select product_id, product_name from {{ ref('dim_products') }}
), 

joined as (
    select 
        b.invoiceno as order_id, 
        b.stockcode as product_id,
        rp.product_name,
        b.invoicedate as order_datetime,
        b.quantity, 
        b.unitprice,
        b.trx_amt
    from base b 
    left join ref_products rp 
        on b.stockcode = rp.product_id
    
)

select * from joined
