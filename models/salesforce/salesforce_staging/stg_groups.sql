
select 

id,
name as full_name

from fivetran_database.salesforce."GROUP"
where type = 'Queue'
