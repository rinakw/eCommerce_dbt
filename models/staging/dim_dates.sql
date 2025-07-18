{{
    config(
        materialized='incremental'
    )
}}

with dates as(
	select 
		distinct(to_date(substring(to_varchar(invoicedate) , 0,10))) as invoice_dates
	from {{ source('orders', 'orders') }}
	where InvoiceMonth = '2010-12'
)

select 
	invoice_dates,
	to_char(invoice_dates, 'Day') as day_of_week,
	extract(day from invoice_dates) as day_of_month,
	extract(month from invoice_dates) as month,
	to_char(invoice_dates, 'Month') as month_name,
	extract(year from invoice_dates) as year,
	case when day_of_week(invoice_dates) in (1,7) then True else False end as is_weekend
from dates
