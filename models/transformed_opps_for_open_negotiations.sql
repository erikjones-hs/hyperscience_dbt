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
'0061R00001A4pwsQAB', /* End date adjustment because of open negotiations. Ascensus 216k */
'006Dm000002dhpbIAA', /* End date adjustment because of open negotiations. CRL 100k */
'0061R00001BAPkAQAX', /* End date adjustment because of open negitotaions. IRS 330k */ 
'0061R00000uL8ylQAC', /* End date adjustment because of open negitotaions. PMP $0 */
'0061R00000yFonNQAS', /* ENd date adjustment because of open negotiations. Metasource $0 */
'0061R000016nZwpQAE', /* End date adjustment because of extension. VetsEZ 500k */
'0061R000014vUKMQA2', /* End date adjustment because of extension. USAF 115k */  
'0061R000014yeOrQAI', /* End date adjustment because of extension. Mathematica 100k */   
'0061R00000zAI8KQAW', /* End date adjustment because of extension. Virginia DMV $0 */ 
'0061R00001A4pwxQAB', /* End date adjustment because of extension. Accerta 81.9k */
'0061R00001A4rKQQAZ'  /* Kovack. Transformed for renewal date purposes only */
)
)

select * from opp_ids