{{
    config(
        materialized='incremental',
        unique_key='invoice_dates'
    )
}}

with dates as(
	select 
		distinct(to_date(substring(to_varchar(invoicedate) , 0,10))) as invoice_dates
	from {{ source('orders', 'orders') }}
	where InvoiceMonth = '2010-12'
), 

extracting as (
    select 
        invoice_dates,
        dayname(invoice_dates) as day_of_week,
        extract(day from invoice_dates) as day_of_month,
        extract(month from invoice_dates) as month,
        extract(year from invoice_dates) as year,
        case when dayname(invoice_dates) in ('Sat','Sun') then True else False end as is_weekend
    from dates
)

select * from extracting
