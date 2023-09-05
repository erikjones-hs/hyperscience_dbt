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
'0061R000014vUKMQA2', /* End date adjustment because of open negotiations. USAF 115k */ 
'0061R000014wI4lQAE', /* End date adjustment because of open negotiations. FATCO 75k */ 
'0061R000014yeOrQAI', /* End date adjustment because of open negotiations. Mathematica 100k */
'0061R00000yGqH3QAK', /* End date adjustment because of open negotiations. SSA 2.3M */
'006Dm000002cdEUIAY', /* End date adjustment because of open negotiations. VA VICCS 1.5M */
'0061R000016nZwpQAE', /* End date adjustment because of open negotiations. VetsEZ 50k */
'0061R00001A5k8bQAB', /* End date adjustment because of open negotiations. SSA 1.45M */
'0061R000014wNrtQAE'  /* End date adjustment because of open negotiations. SSA 1.93M */ 
)
)

select * from opp_ids