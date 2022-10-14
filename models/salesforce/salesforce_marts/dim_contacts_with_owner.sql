with

contacts as (

    select *
    from {{ ref('stg_contacts') }}

),

users as (

    select 

    id as owner_id,
    full_name as owner_full_name,
    role_name as owner_role_name

    from {{ ref('dim_users_and_queues') }}

)

select *
from contacts
left join users 
using (owner_id)
where is_deleted = false