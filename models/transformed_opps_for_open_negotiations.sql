{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* All OPP IDs That have been transformed b/c they are still negotiating renewals */
with opp_ids as (
select distinct opp_id
from {{ref('arr_opp_history')}}
where opp_id in (
'0061R0000136ZbBQAU', /* End date adjustment because of open negotiatios. Reveal 8.8k */
'0061R000014wI4nQAE', /* Consiel 16k */
'0061R000016kwHtQAI', /* End date adjustment because of open negotiations. Miss. Health 689k */ 
'0061R000014wQD2QAM', /* End date adjustment because of open negotiations. VHA 276k */ 
'0061R000014wNsOQAU', /* End date adjustment because of open negotiations. Clean Harbors 228k */ 
'0061R000014wNroQAE', /* End date adjustment because of open negotiations. Pac LIfe 180k */
'0061R000014wNrpQAE', /* End date adjustment because of open negotiations. QAI 35k */ 
'0061R000014vUKMQA2', /* End date adjustment because of open negotiations. USAF 115k */ 
'0061R000014wI4lQAE', /* End date adjustment because of open negotiations. FATCO 75k */ 
'0061R000016myLZQAY', /* End date adjustment because of open negotiations. DOJ 41.5k */ 
'0061R000014yeOrQAI', /* End date adjustment because of open negotiations. Mathematica 100k */
'0061R000014wRB4QAM', /* End date adjustment because of open negotiations. IRS 330k */ 
'0061R000016n7iAQAQ', /* End date adjustment because of open negotiations. Tech Mahindra 125k */
'0061R000014wI4sQAE'  /* End date adjustment because of open negotiations. Canada LIfe 71k */ 
)
)

select * from opp_ids