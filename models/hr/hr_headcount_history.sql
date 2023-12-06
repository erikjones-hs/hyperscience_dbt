{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with raw_data as (
select distinct 
id as employee_id,
to_date(hire_date) as hire_dte,
CASE WHEN termination_date IS NULL then dateadd(days,31,to_date(current_date()))
     ELSE to_date(termination_date) end as end_dte,
date_trunc('month',to_date(hire_dte)) as hire_dte_month,
date_trunc('month',to_date(end_dte)) as end_dte_month
from "FIVETRAN_DATABASE"."BAMBOOHR_FIVETRAN"."EMPLOYEE"
where to_date(hire_dte) <= to_date(current_date()) 
and employment_status in ('Full-Time', 'Terminated')
and last_name not ilike '%(TEST EMPLOYEE)%'
and _fivetran_deleted = FALSE
and employee_id not in (
777,
742,
328,
716,
285,
791,
836,
749,
281,
774,
818,
660,
790    
)
),

months as (
select distinct
date_trunc('month',dte) as date_month
from "DEV"."MARTS"."FY_CALENDAR"
),

employee_months as (
select 
months.date_month,
rd.employee_id,
rd.hire_dte,
rd.hire_dte_month,
rd.end_dte,
rd.end_dte_month
from raw_data as rd
inner join months on (months.date_month >= rd.hire_dte_month AND months.date_month < rd.end_dte_month)
),

joined_employee_months as (
select 
em.date_month,
em.employee_id,
em.hire_dte,
em.hire_dte_month,
em.end_dte,
em.end_dte_month
from employee_months as em
left join raw_data on (em.employee_id = raw_data.employee_id AND em.date_month >= raw_data.hire_dte_month AND em.date_month < raw_data.end_dte_month)
),

final_emp as (
select 
date_month,
employee_id,
hire_dte,
hire_dte_month,
end_dte,
end_dte_month,
to_date(end_dte_month) > to_date(date_month) as is_active,
min(case when is_active then date_month end) over (partition by employee_id) as first_active_month,
max(case when is_active then date_month end) over (partition by employee_id) as last_active_month,
first_active_month = date_month as is_first_month,
last_active_month = date_month as is_last_month
from joined_employee_months
),

churn_emp as (
select 
dateadd(month,1,date_month)::date as date_month,
employee_id,
hire_dte,
hire_dte_month,
end_dte,
end_dte_month,
false as is_active,
first_active_month,
last_active_month,
false as is_first_month,
false as is_last_month
from final_emp
where is_last_month
),

unioned_emp as (
select * from final_emp 
UNION ALL 
select * from churn_emp
),

fct_emp as (
select 
to_timestamp(ue.date_month) as date_month,
ue.employee_id,
to_timestamp(ue.hire_dte) as hire_dte,
to_timestamp(ue.hire_dte_month) as hire_dte_month,
CASE WHEN is_first_month = 'FALSE' then datediff(day,ue.hire_dte,ue.date_month) else 0 end as employee_tenure_calc,
CASE WHEN employee_tenure_calc < 365 then 1 else 0 end as lt_year_flag,
CASE WHEN employee_tenure_calc >= 365 then 1 else 0 end as ge_year_flag,
to_timestamp(ue.end_dte) as end_dte,
to_timestamp(ue.end_dte_month) as end_dte_month,
CASE WHEN ue.is_active = true then 1 else 0 end as is_employee,
ue.is_active,
to_timestamp(ue.first_active_month) as first_active_month,
to_timestamp(ue.last_active_month) as last_active_month,
ue.is_first_month,
ue.is_last_month
from unioned_emp as ue
order by employee_id, date_month asc
)

select * from fct_emp