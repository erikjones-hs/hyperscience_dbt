{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with opp as (
select distinct
opp_id,
opp_is_partner_influenced_flag,
date_ran
from {{ ref('agg_opportunity_incremental') }}
where to_date(date_ran) = to_date(current_date()) -1 
),

opps as (
select distinct
fao.opp_id,
fao.opp_name,
fao.account_id,
fao.account_name,
opp.opp_is_partner_influenced_flag
from {{ ref('fct_arr_opp') }} as fao
left join opp on (fao.opp_id = opp.opp_id) 
),

acct_hist as (
select distinct
account_id,
account_name,
sum(opp_is_partner_influenced_flag) as partner_flag
from opps
group by 1,2
order by account_id, account_name
),

categories as (
select distinct
opp_id,
opp_name,
account_name,
partner_sourced_influenced,
bpo,
direct_deal
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."ARR_CATEGORIES"
),

combined as (
select distinct
ah.account_id,
ah.account_name,
cat.partner_sourced_influenced,
cat.bpo,
cat.direct_deal,
ah.partner_flag
from acct_hist as ah 
left join categories as cat on (ah.account_name = cat.account_name)
order by ah.account_id
),

clean as (
select distinct
account_id,
account_name,
CASE WHEN (bpo IS NULL AND direct_deal IS NULL AND (partner_sourced_influenced IS NOT NULL or partner_flag >= 1)) then 1 else 0 end as is_partner,
CASE WHEN bpo IS NOT NULL then 1 else 0 end as is_bpo,
CASE WHEN direct_deal IS NOT NULL or (is_partner IS NULL and is_bpo IS NULL) then 1 else 0 end as is_direct_int,
(is_partner + is_bpo + is_direct_int) as cat_sum,
CASE WHEN is_direct_int = 1 or cat_sum = 0 then 1 else 0 end as is_direct
from combined
order by account_id
),

fct_deal_type as (
select distinct
account_id,
account_name,
CASE WHEN is_partner = 1 then 'partner'
     WHEN is_bpo = 1 then 'bpo '
     WHEN is_direct = 1 then 'direct'
     ELSE 'other' end as deal_type
from clean
order by account_id
)

select * from fct_deal_type
