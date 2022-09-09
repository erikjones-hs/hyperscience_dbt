
with

leads as (

    select *
    from {{ ref('stg_leads') }}

),

users as (

    select 

    id as owner_id,
    full_name as owner_full_name,
    role_name as owner_role_name

    from {{ ref('dim_users_and_queues') }}

)

select *
from leads
left join users 
using (owner_id)
where is_deleted = false