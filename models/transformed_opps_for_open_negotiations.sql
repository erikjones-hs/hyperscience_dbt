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
'0061R000014yeOrQAI', /* End date adjustment because of open negotiations. Mathematica 100k */
'006Dm000002cdEUIAY', /* End date adjustment because of open negotiations. VA VICCS 1.5M */
'0061R00001A4pwsQAB', /* End date adjustment because of open negotiations. Ascensus 216k */
'0061R000014wI4uQAE', /* End date adjustment because of open negotiations. AIG 528k */
'0061R00001A4pwYQAR', /* End date adjustment because of open negotiations. Unum 690k */
'006Dm000002dhpbIAA', /* End date adjustment because of open negotiations. CRL 100k */
'0061R000019PUmDQAW', /* End date adjustment because of open negtiations. VA Conslidated Contract 5.1M */
'0061R00001BAPkAQAX'  /* End date adjustment because of open negitotaions. IRS 330k */ 
)
)

select * from opp_ids