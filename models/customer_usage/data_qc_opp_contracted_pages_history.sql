{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CUSTOMER_USAGE'
)
}} 

with opp_contracted_pages_history as (
select distinct
opp_id,
opp_name,
contracted_pages
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."OPP_CONTRACTED_PAGES_HISTORY"
),

fct_arr_opp as (
select distinct
date_month,
opp_id,
opp_name,
account_id,
account_name,
start_dte,
end_dte,
is_active,
is_active_acct
from {{ref('fct_arr_opp')}}
where to_date(date_month) <= date_trunc(month,to_date(current_date))
order by account_id, opp_id, date_month asc
),

fct_arr_opp_contracted_pages as (
select distinct
fao.date_month,
fao.opp_id,
fao.opp_name,
fao.account_id,
fao.account_name,
fao.start_dte,
fao.end_dte,
fao.is_active,
fao.is_active_acct,
ocph.contracted_pages
from fct_arr_opp as fao
left join opp_contracted_pages_history as ocph on (fao.opp_id = ocph.opp_id)
order by fao.account_id, fao.opp_id, fao.date_month asc 
)

select distinct 
opp_id,
opp_name,
account_id,
account_name
from fct_arr_opp_contracted_pages
where is_active = TRUE
and contracted_pages IS NULL