{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'HR'
)
}}

with emp as (
select distinct
id as employee_eid,
first_name as employee_first_name,
last_name as employee_last_name,
full_name_1 as employee_name,
employee_number,
to_timestamp(hire_date) as employee_hire_dte,
CASE WHEN termination_date = '0000-00-00' then datediff(days,to_date(hire_date),to_date(current_date())) 
     WHEN termination_date != '0000-00-00' then datediff(days,to_date(hire_date),to_date(termination_date)) 
     else 'NA' end as employee_tenure_days,
department as employee_department,
work_email as employee_work_email,
city as employee_city,
state as employee_state,
country as employee_country,
location as employee_location,
job_title as employee_job_title,
pay_type as employee_pay_type,
pay_rate as employee_pay_rate,
to_timestamp(pay_rate_effective_date) as employee_pay_rate_efective_date,
pay_change_reason as employee_pay_change_reason,
date_of_birth as employee_dob,
age as employee_age,
ethnicity as employee_ethnicity,
gender as employee_gender,
status as employee_status,
termination_date as employee_termination_dte,
supervisor_eid,
supervisor_email,
supervisor
from "FIVETRAN_DATABASE"."BAMBOOHR"."EMPLOYEE"
),

current_status as (
select distinct
employee_id,
status_effective_dte,
current_status
from "DEV"."HR"."BAMBOO_FCT_EMPLOYEE_CURRENT_STATUS"
),

current_level as (
select * from (
select distinct  
employee_id,
level_effective_dte,
professional_band,
emp_level,
row_number () over (partition by employee_id order by level_effective_dte desc) as row_num
from "DEV"."HR"."BAMBOO_FCT_EMP_LEVEL_HIST"
order by employee_id, level_effective_dte desc)
where row_num = 1  
),

current_salary as (
select * from (
select distinct  
employee_id,
pay_effective_dte,
salary_usd,
row_number () over (partition by employee_id order by pay_effective_dte desc) as row_num
from "DEV"."HR"."BAMBOO_FCT_COMPENSATION_HIST"
order by employee_id, pay_effective_dte desc)
where row_num = 1  
),

current_job as (
select * from (
select distinct  
employee_id,
date,
job_title,
location,
reports_to,
department,
row_number () over (partition by employee_id order by date desc) as row_num
from "DEV"."HR"."BAMBOO_FCT_JOB_HIST"
order by employee_id, date desc)
where row_num = 1  
),

dim_employee as (
select
emp.employee_eid,
emp.employee_first_name,
emp.employee_last_name,
emp.employee_name,
emp.employee_number,
emp. employee_hire_dte,
emp.employee_tenure_days,
emp.employee_department,
emp.employee_work_email,
emp.employee_city,
emp.employee_state,
emp.employee_country,
emp.employee_location,
to_timestamp(cj.date) as employee_job_title_effective_dte,
cj.job_title as employee_job_title,
emp.employee_pay_type,
emp.employee_pay_rate,
to_timestamp(c_sal.pay_effective_dte) as employee_pay_effective_dte,
c_sal.salary_usd as employee_salary_usd,
emp.employee_pay_change_reason,
to_timestamp(cl.level_effective_dte) as employee_level_effective_dte,
cl.professional_band as employee_band,
cl.emp_level as employee_level,
to_timestamp(cs.status_effective_dte) as employee_status_effective_dte,
cs.current_status as employee_current_status,
emp.employee_status as employee_active_inactive,
emp.employee_dob,
emp.employee_age,
emp.employee_ethnicity,
emp.employee_gender,
emp.employee_termination_dte,
emp.supervisor_eid,
cj.reports_to as supervisor,
emp.supervisor_email,
to_date(current_date()) as date_ran
from emp
left join current_status as cs on (emp.employee_eid = cs.employee_id)
left join current_level as cl on (emp.employee_eid = cl.employee_id)
left join current_salary as c_sal on (emp.employee_eid = c_sal.employee_id)
left join current_job as cj on (emp.employee_eid = cj.employee_id)
)

select * from dim_employee order by employee_eid