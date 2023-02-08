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

),

users_1 as (

    select 

    id as created_by_id,
    full_name as created_by_full_name,
    role_name as created_by_role_name

    from {{ ref('dim_users_and_queues') }}

)

select *
from contacts
left join users 
using (owner_id)
left join users_1
using (created_by_id)
where is_deleted = false