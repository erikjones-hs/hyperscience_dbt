{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with raw_data as (
select distinct 
employee_eid,
to_date(employee_hire_dte) as hire_dte,
CASE WHEN employee_termination_dte = '0000-00-00' then to_date('2022-12-31')
     ELSE to_date(employee_termination_dte) end as end_dte,
date_trunc('month',to_date(employee_hire_dte)) as hire_dte_month,
date_trunc('month',to_date(end_dte)) as end_dte_month,
employee_salary_usd
from "PROD"."HR"."BAMBOO_DIM_EMPLOYEE"
where to_date(date_ran) = dateadd(day,-1,to_date(current_date())) 
),

months as (
select distinct
date_trunc('month',dte) as date_month
from "DEV"."MARTS"."FY_CALENDAR"
),

employee_months as (
select 
months.date_month,
rd.employee_eid,
rd.hire_dte,
rd.hire_dte_month,
rd.end_dte,
rd.end_dte_month,
rd.employee_salary_usd
from raw_data as rd
inner join months on (months.date_month >= rd.hire_dte_month AND months.date_month < rd.end_dte_month)
),

joined_employee_months as (
select 
em.date_month,
em.employee_eid,
em.hire_dte,
em.hire_dte_month,
em.end_dte,
em.end_dte_month,
em.employee_salary_usd,
coalesce(raw_data.employee_salary_usd,0) as salary
from employee_months as em
left join raw_data on (em.employee_eid = raw_data.employee_eid AND em.date_month >= raw_data.hire_dte_month AND em.date_month < raw_data.end_dte_month)
),

final_emp as (
select 
date_month,
employee_eid,
hire_dte,
hire_dte_month,
end_dte,
end_dte_month,
employee_salary_usd,
salary,
to_date(end_dte_month) > to_date(date_month) as is_active,
min(case when is_active then date_month end) over (partition by employee_eid) as first_active_month,
max(case when is_active then date_month end) over (partition by employee_eid) as last_active_month,
first_active_month = date_month as is_first_month,
last_active_month = date_month as is_last_month
from joined_employee_months
),

churn_emp as (
select 
dateadd(month,1,date_month)::date as date_month,
employee_eid,
hire_dte,
hire_dte_month,
end_dte,
end_dte_month,
0::float as employee_salary_usd,
0::float as salary,
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

emp_meta_data as (
select distinct
employee_eid,
employee_name,
employee_tenure_days,
employee_department,
employee_location,
employee_job_title,
supervisor,
supervisor_email
from "PROD"."HR"."BAMBOO_DIM_EMPLOYEE"
where to_date(date_ran) = dateadd(day,-1,to_date(current_date())) 
),

fct_emp as (
select 
to_timestamp(ue.date_month) as date_month,
ue.employee_eid,
to_timestamp(ue.hire_dte) as hire_dte,
to_timestamp(ue.hire_dte_month) as hire_dte_month,
CASE WHEN is_first_month = 'FALSE' then datediff(day,ue.hire_dte,ue.date_month) else 0 end as employee_tenure_calc,
CASE WHEN employee_tenure_calc < 365 then 1 else 0 end as lt_year_flag,
CASE WHEN employee_tenure_calc >= 365 then 1 else 0 end as ge_year_flag,
to_timestamp(ue.end_dte) as end_dte,
to_timestamp(ue.end_dte_month) as end_dte_month,
ue.employee_salary_usd,
ue.salary,
CASE WHEN ue.is_active = true then 1 else 0 end as is_employee,
ue.is_active,
to_timestamp(ue.first_active_month) as first_active_month,
to_timestamp(ue.last_active_month) as last_active_month,
ue.is_first_month,
ue.is_last_month,
emd.employee_name,
emd.employee_tenure_days,
emd.employee_department,
emd.employee_location,
emd.employee_job_title,
emd.supervisor,
emd.supervisor_email
from unioned_emp as ue
left join emp_meta_data as emd on (ue.employee_eid = emd.employee_eid)
order by employee_eid, date_month asc
)

select * from fct_emp