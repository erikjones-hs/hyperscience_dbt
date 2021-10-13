{{ config
(
    materialized='incremental',
    database = 'PROD',
    schema = 'HR'
)
}}

with recursive managers (employee_id, manager_id) as 
(select 
id as employee_id, 
supervisor_eid as manager_id
from "FIVETRAN_DATABASE"."BAMBOOHR"."EMPLOYEE"
where job_title = 'CEO'
union all
select 
employees.id as employee_id, 
employees.supervisor_eid as manager_id
from "FIVETRAN_DATABASE"."BAMBOOHR"."EMPLOYEE" as employees 
join managers on (employees.supervisor_eid = managers.employee_id)
),

fct_dim_reporting as (
select 
employee_id,
manager_id,
to_date(current_date()) as date_ran
from managers
order by manager_id nulls first, employee_id
)

select * from fct_dim_reporting

{% if is_incremental() %}

  where date_ran >= (select max(date_ran) from {{ this }})

{% endif %}