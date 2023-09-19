{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Pulling in transformed SFDC data from arr_opp_history_transform model*/
with raw_data_transformed as (
select * from {{ref('arr_opp_history_transformed')}}
),

/* Subscription period intermediate view */
/* each row is an opportunity with transformed start and end dates */
/* one last adjustment to end months to account for opportunities that start on the 1st of a month and end on the last day of the month */
subscription_periods as (
select 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
start_dte_month,  
start_dte,
end_dte,
CASE WHEN (end_dte = last_day(end_dte_month) and start_dte = start_dte_month) then dateadd(month,1,end_dte_month)  
     else end_dte_month end as end_dte_month,
opp_arr,
opp_net_new_arr
from raw_data_transformed
),

/* dim dates table */
months as (
select distinct
date_trunc('month',dte) as date_month
from "DEV"."MARTS"."FY_CALENDAR"
),

/* each row in this table is an oppportunity - month combination */
/* making some further historical adjustments to ARR per FP&A */
opportunity_months as (
select 
months.date_month,
sp.account_id,
sp.account_name,
sp.opp_id,
sp.opp_name,
sp.opp_revenue_type,
sp.start_dte_month,
sp.start_dte,
sp.end_dte_month,
sp.end_dte,
sp.opp_arr,
sp.opp_net_new_arr
from subscription_periods as sp
inner join months on (months.date_month >= sp.start_dte_month AND months.date_month < sp.end_dte_month)
),

/* fanning out opportunities by month */
/* each row is an opporunity - month combination with zeros filled in for missing months */
/* each opportunity should have a record every month, for months between it's start month and end month */ 
joined_opportunity_int as (
select 
om.date_month,
om.account_id,
om.account_name,
om.opp_id,
om.opp_name,
om.opp_revenue_type,
om.start_dte_month,
om.start_dte,
om.end_dte_month,
om.end_dte,
om.opp_arr,
om.opp_net_new_arr,
coalesce(subscription_periods.opp_arr,0) as mrr
from opportunity_months as om
left join subscription_periods on (om.opp_id = subscription_periods.opp_id AND om.date_month >= subscription_periods.start_dte_month AND om.date_month < subscription_periods.end_dte_month)
),

/* making final adjustmnents to historical mrr per FP&A */
joined_opportunity as (
select
joi.date_month,
joi.account_id,
joi.account_name,
joi.opp_id,
joi.opp_name,
joi.opp_revenue_type,
joi.start_dte_month,
joi.start_dte,
joi.end_dte_month,
joi.end_dte,
joi.opp_arr,
joi.opp_net_new_arr,
CASE WHEN opp_id = '0063600000X36zWAAR' and to_date(joi.date_month) <= '2019-06-01' then 128000 
     WHEN opp_id = '0063600000dsPsjAAE' and to_date(joi.date_month) <= '2019-07-01' then 560000 
     when opp_id = '0061R00000r6r1iQAA' and to_date(joi.date_month) >= '2021-11-01' then 0.00000000001
     when opp_id = '0061R00000uL8ylQAC' and to_date(joi.date_month) >= '2021-11-01' then 0.00000000001
     when opp_id = '0061R00000yFonNQAS' and to_date(joi.date_month) >= '2022-05-01' then 0.00000000001
     when opp_id = '0061R00000zAI8KQAW' and to_date(joi.date_month) >= '2022-05-01' then 0.00000000001
     when opp_id = '0061R00000yEQVgQAO' and to_date(joi.date_month) >= '2022-05-01' then 0.00000000001
     when opp_id = '0061R0000137hQzQAI' and to_date(joi.date_month) >= '2022-04-01' then 15000 /* Updated based on agreement to settle per FP&A */ 
     when opp_id = '0061R00001A3ujGQAR' and to_date(joi.date_month) <= '2022-07-01' then 300000
     when opp_id = '0061R000014wNrUQAU' and to_date(joi.date_month) >= '2022-12-01' then 145000
     when opp_id = '0061R00001A4rFVQAZ' and to_date(joi.date_month) >= '2022-12-01' then 253000
     when opp_id = '0061R000014wI4hQAE' and to_date(joi.date_month) >= '2022-12-01' then 20000
     when opp_id = '0061R000014vUKMQA2' and to_date(joi.date_month) >= '2023-08-01' then 86250
     when opp_id = '0061R000014yeOrQAI' and to_date(joi.date_month) >= '2023-08-01' then 75000
     when opp_id = '0061R000014wNrtQAE' and to_date(joi.date_month) >= '2023-09-01' then 1106559
     ELSE joi.mrr end as mrr
from joined_opportunity_int as joi 
),

/* adding in binary features for active, first active month, last active month, first month and last month */
final_opp as (
select 
date_month,
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
start_dte_month,
start_dte,
end_dte_month,
end_dte,
opp_arr,
opp_net_new_arr,
mrr,
mrr > 0 as is_active, 
min(case when is_active then date_month end) over (partition by opp_id) as first_active_month,
max(case when is_active then date_month end) over (partition by opp_id) as last_active_month,
first_active_month = date_month as is_first_month,
last_active_month = date_month as is_last_month
from joined_opportunity as jo
),

/* identifying churned month for each opportunity */
churn_opp as (
select 
dateadd(month,1,date_month)::date as date_month,
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
start_dte_month,
start_dte,
end_dte_month,
end_dte,
0::float as opp_arr,
0::float as opp_net_new_arr,
0::float as mrr,
false as is_active,
first_active_month,
last_active_month,
false as is_first_month,
false as is_last_month
from final_opp
where is_last_month
),

/* combining non-churned months with churned month for opportunities */
/* last entry for each opp id is the churn month, zero'ing out mrr */
/* now we have a full historical transaction record for each opp id */
/* first record is the start month, last record is the churn month with zero'ed out mrr */
unioned_opp as (
select * from final_opp 
UNION ALL 
select * from churn_opp
),

/* for any opportunity - month, looking back one month and forward one month to be able to classify mrr changes correctly */
mrr_with_changes_opp as (
select 
date_month,
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
start_dte_month,
start_dte,
end_dte_month,
end_dte,
opp_arr,
opp_net_new_arr,
mrr,
is_active, 
first_active_month,
last_active_month,
is_first_month,
is_last_month,
coalesce(lag(is_active) over (partition by opp_id order by date_month),false) as previous_month_is_active,
coalesce(lead(is_active) over (partition by opp_id order by date_month),false) as next_month_is_active,
coalesce(lag(mrr) over (partition by opp_id order by date_month),0) as previous_month_mrr,
coalesce(lead(mrr) over (partition by opp_id order by date_month),0) as next_month_mrr,
mrr - previous_month_mrr as mrr_change
from unioned_opp
),

/* ######################################################################## */
/* ######################################################################## */
/* NOW DOING THE SAME THING FOR ACCOUNTS THAT WE JUST DID FOR OPPORTUNITIES */

/* summing mrr by account and month */
/* identifying an active account */
account_int as (
select distinct 
date_month,
account_id,
CASE WHEN sum(mrr) over (partition by account_id, date_month) > 0 THEN true else false end as is_active_acct,
sum(mrr) over (partition by account_id, date_month) as account_mrr 
from final_opp
),

/* idnetifying first and last active months for an account */
account as (
select distinct 
date_month,
account_id,
is_active_acct,
account_mrr,
min(case when is_active_acct then date_month end) over (partition by account_id) as first_active_month_acct,
max(case when is_active_acct then date_month end) over (partition by account_id) as last_active_month_acct
from account_int
),

/* adding binary identifiers for is_first_month and is_last_month for accounts */
final_acct as (
select distinct
date_month,
account_id,
is_active_acct,
account_mrr,
first_active_month_acct,
last_active_month_acct,
first_active_month_acct = date_month as is_first_month_acct,
last_active_month_acct = date_month as is_last_month_acct
from account 
),

/* identifying churned month for each opportunity */
churn_acct as (
select 
dateadd(month,1,date_month)::date as date_month,
account_id,
false as is_active_acct,
0::float as account_mrr,
first_active_month_acct,
last_active_month_acct,
false as is_first_month_acct,
false as is_last_month_acct
from final_acct
where is_last_month_acct
),

/* combining non-churned months with churned month for accounts */
/* last entry for each account id is the churn month, zero'ing out mrr */
/* now we have a full historical transaction record for each account id */
/* first record is the start month, last record is the churn month with zero'ed out mrr */
unioned_acct as (
select * from final_acct
UNION ALL 
select * from churn_acct
),

/* for any opportunity - month, looking back one month and forward one month to be able to classify mrr changes correctly */
mrr_with_changes_acct as (
select 
date_month,
account_id,
is_active_acct,
account_mrr,
first_active_month_acct,
last_active_month_acct,
is_first_month_acct,
last_active_month_acct = date_month as is_last_month_acct,
coalesce(lag(is_active_acct) over (partition by account_id order by date_month),false) as previous_month_is_active_acct,
coalesce(lead(is_active_acct) over (partition by account_id order by date_month),false) as next_month_is_active_acct,
coalesce(lag(account_mrr) over (partition by account_id order by date_month),0) as previous_month_mrr_acct,
coalesce(lead(account_mrr) over (partition by account_id order by date_month),0) as next_month_mrr_acct,
account_mrr - previous_month_mrr_acct as mrr_change_acct
from unioned_acct
),

/* joining opportunity and account data */
change_table_int as (
select
mwco.date_month,
mwco.account_id,
mwco.account_name,
mwco.opp_id,
mwco.opp_name,
mwco.opp_revenue_type,
mwco.start_dte_month,
mwco.start_dte,
mwco.end_dte_month,
mwco.end_dte,
mwco.opp_arr,
mwco.opp_net_new_arr,
mwco.mrr,
mwco.is_active,
mwco.first_active_month,
mwco.last_active_month,
mwco.is_first_month,
mwco.is_last_month,
mwco.previous_month_is_active,
mwco.next_month_is_active,
mwco.previous_month_mrr,
mwco.next_month_mrr,
mwco.mrr_change,
mwca.is_active_acct,
mwca.first_active_month_acct,
mwca.last_active_month_acct,
mwca.is_first_month_acct,
mwca.is_last_month_acct,
mwca.account_mrr,
mwca.previous_month_is_active_acct,
mwca.next_month_is_active_acct,
mwca.previous_month_mrr_acct,
mwca.next_month_mrr_acct,
mwca.mrr_change_acct
from mrr_with_changes_opp as mwco
left join mrr_with_changes_acct as mwca on (mwco.account_id = mwca.account_id AND mwco.date_month = mwca.date_month) 
),

/* categorizing opportunity, revenue and account changes */
change_table as (
select distinct 
date_month,
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
start_dte_month,
start_dte,
end_dte_month,
end_dte,
opp_arr,
opp_net_new_arr,
mrr,
is_active,
first_active_month,
last_active_month,
is_first_month,
is_last_month,
previous_month_is_active,
next_month_is_active,
previous_month_mrr,
next_month_mrr,
mrr_change,
is_active_acct,
first_active_month_acct,
last_active_month_acct,
is_first_month_acct,
is_last_month_acct,
account_mrr,
previous_month_is_active_acct,
next_month_is_active_acct,
previous_month_mrr_acct,
next_month_mrr_acct,
mrr_change_acct,
CASE WHEN is_first_month = true and mrr > 0 then 'new'
     WHEN is_active = false and previous_month_is_active = true then 'churn'
     ELSE 'recurring'
     END AS opp_category,
CASE WHEN is_first_month_acct = true then 'new'
     WHEN is_active_acct = false and previous_month_is_active_acct = true and next_month_is_active_acct = false then 'churn'
     WHEN (is_active_acct = true OR (is_active_acct = false AND previous_month_is_active_acct = true AND next_month_is_active_acct = true)) then 'active'
     ELSE NULL
     END AS customer_category,
CASE WHEN customer_category = 'new' and mrr_change_acct > 0 then 'new'
     WHEN customer_category = 'active' and mrr_change_acct = 0 then 'recurring'
     WHEN customer_category = 'active' and mrr_change_acct > 0 then 'expansion'
     WHEN mrr_change_acct < 0 then 'churn'
     ELSE NULL end as revenue_category
from change_table_int
),

/* removing unnecessary columns from the final table */
/* renaming so column names are consistent */
fct_arr as (
select distinct
to_timestamp(date_month) as date_month,
account_id,
account_name,
opp_id,
opp_name,
to_timestamp(start_dte_month) as start_dte_month,
to_timestamp(start_dte) as start_dte,
to_timestamp(end_dte_month) as end_dte_month,
to_timestamp(end_dte) as end_dte,
mrr,
mrr_change,
CASE WHEN mrr = 0 then mrr_change else mrr end as mrr_reporting,
is_active,
first_active_month,
last_active_month,
is_first_month,
is_last_month,
account_mrr as mrr_acct,
mrr_change_acct,
CASE WHEN mrr_acct = 0 then mrr_change_acct else mrr_acct end as mrr_reporting_acct,
is_active_acct,
first_active_month_acct,
last_active_month_acct,
is_first_month_acct,
is_last_month_acct,
CASE WHEN opp_id = '0061R000010t71kQAA' and to_date(date_month) = '2022-01-01' then 'de-book' /* Sience SAS */
     when opp_id = '0061R000014vnNlQAI' and to_date(date_month) = '2023-01-01' then 'de-book' /* i3 Systems */
     when opp_id = '0061R000013gijQQAQ' and to_date(date_month) = '2022-11-01' then 'de-book' /* MindMap */
     when opp_id = '0061R000016mzrWQAQ' and to_date(date_month) = '2022-11-01' then 'de-book' /* FeatSystems */
     when opp_id = '0061R0000137scfQAA' and to_date(date_month) = '2022-11-01' then 'de-book' /* Cogent */
     else opp_category end as opp_category,
CASE WHEN opp_id = '0061R000010t71kQAA' and to_date(date_month) = '2022-01-01' then 'de-book' /* Sience SAS */
     when opp_id = '0061R000014vnNlQAI' and to_date(date_month) = '2023-01-01' then 'de-book' /* i3 Systems */
     when opp_id = '0061R000013gijQQAQ' and to_date(date_month) = '2022-11-01' then 'de-book' /* MindMap */
     when opp_id = '0061R000016mzrWQAQ' and to_date(date_month) = '2022-11-01' then 'de-book' /* FeatSystems */
     when opp_id = '0061R0000137scfQAA' and to_date(date_month) = '2022-11-01' then 'de-book' /* Cogent */
     else customer_category end as customer_category,
CASE WHEN opp_id = '0061R000010t71kQAA' and to_date(date_month) = '2022-01-01' then 'de-book' /* Sience SAS */
     when opp_id = '0061R000014vnNlQAI' and to_date(date_month) = '2023-01-01' then 'de-book' /* i3 Systems */
     when opp_id = '0061R000013gijQQAQ' and to_date(date_month) = '2022-11-01' then 'de-book' /* MindMap */
     when opp_id = '0061R000016mzrWQAQ' and to_date(date_month) = '2022-11-01' then 'de-book' /* FeatSystems */
     when opp_id = '0061R0000137scfQAA' and to_date(date_month) = '2022-11-01' then 'de-book' /* Cogent */
     else revenue_category end as revenue_category
from change_table
order by account_id, start_dte_month asc, date_month asc
)

select * from fct_arr
