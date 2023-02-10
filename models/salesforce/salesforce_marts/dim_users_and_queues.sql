with 

users as (

    select *
    from {{ ref('stg_users') }}

),

queues as (

    select *
    from {{ ref('stg_groups') }}

)

select 

id,
full_name,
role_name,
department

from users
union all
select 

id,
full_name,
null as role_name,
null as department

from queues