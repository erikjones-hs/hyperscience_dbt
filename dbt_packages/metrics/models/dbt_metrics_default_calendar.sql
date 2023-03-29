{{ config(materialized='table') }}

with days as (
    {{ metrics.metric_date_spine(
    datepart="day",
    start_date="cast('1990-01-01' as date)",
    end_date="cast('2050-02-28' as date)"
   )
    }}
),

dates as (
    select 
        cast(date_day as date) as date_day,
        cast({{ date_trunc('week', 'date_day') }} as date) as date_week,
        cast({{ date_trunc('month', 'date_day') }} as date) as date_month,
        cast({{ date_trunc('quarter', 'date_day') }} as date) as date_quarter,
        cast({{ date_trunc('year', 'date_day') }} as date) as date_year
    from days
),

fy as (
select
date_day,
CASE WHEN month(date_month) in (3,4,5) then 'Q1'
     WHEN month(date_month) in (6,7,8) then 'Q2'
     WHEN month(date_month) in (9,10,11) then 'Q3'
     WHEN month(date_month) in (12,1,2) then 'Q4'
     ELSE 'NA' end as date_fiscal_quarter_num,
CASE WHEN month(date_month) in (1,2) then year(dateadd('year',-1,date_trunc(year,date_day))) ELSE year(date_trunc(year,date_day)) end as date_fy_int,
to_variant(concat(date_fy_int,'-','01','-','01')) as date_fy,
concat(date_fiscal_quarter_num,'-',date_fy_int) as date_fy_qtr_year
from dates
order by date_day asc
),

fy_int as (
select 
d.*,
fy.date_fiscal_quarter_num,
to_date(fy.date_fy) as date_fy,
fy.date_fy_qtr_year
from dates as d
left join fy on d.date_day = fy.date_day
order by d.date_day asc
),

qtr_end as (
select 
date_fy_qtr_year,
max(date_day) as date_fiscal_qtr_end_dte
from fy_int
group by 1 
order by split_part(date_fy_qtr_year,'-',2) asc, split_part(date_fy_qtr_year,'-',1) asc
),

dim_dates as (
select 
fy_int.*,
qtr_end.date_fiscal_qtr_end_dte
from fy_int
left join qtr_end on (fy_int.date_fy_qtr_year = qtr_end.date_fy_qtr_year)
)

select * from dim_dates
