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
'0061R000016kwHtQAI', /* End date adjustment because of open negotiations. Miss. Health 689k */ 
'0061R000014wNrpQAE', /* End date adjustment because of open negotiations. QAI 35k */ 
'0061R000014vUKMQA2', /* End date adjustment because of open negotiations. USAF 115k */ 
'0061R000014wI4lQAE', /* End date adjustment because of open negotiations. FATCO 75k */ 
'0061R000016myLZQAY', /* End date adjustment because of open negotiations. DOJ 41.5k */ 
'0061R000014yeOrQAI', /* End date adjustment because of open negotiations. Mathematica 100k */
'0061R000014wI4sQAE' /* End date adjustment because of open negotiations. Canada LIfe 71k */ 
)
)

select * from opp_ids