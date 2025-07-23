{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with 
base as (
    select 
        o.invoiceno , 
        o.invoicedate, 
        o.quantity, 
        o.unitprice, 
        o.customerid, 
        o.country,
        (o.quantity * o.unitprice) as trx_amt,
        case 
            when substr(o.invoiceno,1,1) = 'C' then 'C'
            when substr(o.invoiceno,1,1) = 'A' then 'A' 
            else 'X'
        end as invoice_prefix
    from
        {{ source('orders', 'orders') }} o
    
),

add_status as (
    select 
        o.invoiceno , 
        o.invoicedate,
        o.customerid, 
        o.country,
        o.trx_amt,
        s.order_status
    from base o
    left join {{ ref('order_status') }} s
        on o.invoice_prefix =  s.order_code
),

ref_country as (
    select * from {{ ref('dim_country') }} 
),

ref_dates as (
    select * from {{ ref('dim_dates') }} 

),

aggr as (
    select 
        s.invoiceno ,
        s.invoicedate, 
        s.customerid,
        s.country,
        s.order_status,
        sum(s.trx_amt) as tot_amt,
    from add_status s
    group by 
        1,2,3,4,5
),

joined as(
    select 
        a.invoiceno as order_id,
        a.invoicedate as order_date, 
        a.customerid as customer_id,
        a.tot_amt,
        a.order_status,
        a.country,
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