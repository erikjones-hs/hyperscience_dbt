{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with  nrr_opps_closed_won as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
opp_services_nrr,
opp_stage_name,
opp_start_dte,
opp_close_dte
from {{ ref('agg_opportunity_incremental') }}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
and opp_stage_name in ('Closed Won')
and to_date(opp_close_dte) >= '2020-03-01'
and to_date(opp_close_dte) <= to_date(current_date())
and opp_services_nrr > 0
order by opp_start_dte asc
),

nrr_closed_won_int1 as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
/* Zeroing Out NRR because we didn;t have is_deleted = FALSE in the agg opp incremental code */
CASE WHEN opp_id = '0061R00000r7xPhQAI' then 0
     WHEN opp_id = '0061R00000uLcG2QAK' then 0
     WHEN opp_id = '0061R000010seZeQAI' then 0
     WHEN opp_id = '0061R000010shu0QAA' then 30000
     WHEN opp_id = '0061R00000zAM8wQAG' then 176706
     WHEN opp_id = '0061R000014wI4uQAE' then 0
     WHEN opp_id = '0061R00001BAun6QAD' then 12000
     WHEN opp_id = '006Dm000002gMTLIA2' then 18500
     WHEN opp_id = '0061R000014wK2vQAE' then 0
     WHEN opp_id = '0061R000014yJQFQA2' then 0
     WHEN opp_id = '0061R000014yfmYQAQ' then 0
     WHEN opp_id = '0061R000019R8fwQAC' then 0
     when opp_id = '0061R00000oE2hbQAC' then 0
     when opp_id = '0061R0000137hOKQAY' then 0
     else ZEROIFNULL(opp_services_nrr) end as opp_services_nrr,
opp_start_dte,
opp_close_dte
from nrr_opps_closed_won
order by opp_start_dte asc
),

nrr_closed_won as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
opp_start_dte,
sum(opp_services_nrr) as opp_services_nrr
from nrr_closed_won_int1
group by account_id, account_name, opp_id, opp_start_dte, opp_name
order by opp_start_dte
)

select * from nrr_closed_won