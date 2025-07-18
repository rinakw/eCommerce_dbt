{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with 
base as (
    select 
        invoiceno , 
        invoicedate, 
        quantity, 
        unitprice, 
        customerid, 
        country,
        (quantity * unitprice) as trx_amt
    from
        {{ source('orders', 'orders') }} 
),

ref_country as (
    select * from {{ ref('dim_country') }} 
),

ref_dates as (
    select * from {{ ref('dim_dates') }} 

),

aggr as (
    select 
        b.invoiceno ,
        b.invoicedate, 
        b.customerid,
        b.country,
        sum(b.trx_amt) as tot_amt
    from base b
    group by 
        1,2,3,4
),

joined as(
    select 
        a.invoiceno as order_id,
        a.invoicedate as order_date, 
        a.customerid as customer_id,
        a.country,
        a.tot_amt,
        rc.iso_code,
        rc.region, 
        rc.sub_region, 
        rc.latitude, 
        rc.longitude,
        rd.is_weekend
    from aggr a
    left join ref_country rc
        on rc.country_name = a.country
    left join ref_dates rd 
        on rd.invoice_dates = a.invoicedate

)

select * from joined