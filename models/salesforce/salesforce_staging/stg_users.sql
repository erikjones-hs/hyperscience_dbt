
with

users as (

    select 

    id,
    username,
    first_name,
    last_name,
    name as full_name,
    email,
    user_role_id,
    is_active,
    department

    from {{ source('salesforce', 'user')}}

),

user_roles as (

    select 

    id,
    name as role_name


    from {{ source('salesforce', 'user_role') }}

)

select 

 users.id,
 username,
 first_name,
 last_name,
 full_name,
 email,
 role_name,
 is_active,
 department

 from users 
 left join user_roles 
 on users.user_role_id = user_roles.id


