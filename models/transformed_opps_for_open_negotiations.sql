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
'006Dm000002dhpbIAA', /* End date adjustment because of open negotiations. CRL 100k */ 
'0061R00000uL8ylQAC', /* End date adjustment because of open negitotaions. PMP $0 */
'0061R00000yFonNQAS', /* ENd date adjustment because of open negotiations. Metasource $0 */
'0061R000014vUKMQA2', /* End date adjustment because of extension. USAF 115k */  
'0061R000014yeOrQAI', /* End date adjustment because of extension. Mathematica 100k */   
'0061R00001A4rKQQAZ', /* Kovack. Transformed for renewal date purposes only */
'0061R00001A4rFVQAZ', /* End date adjustment because of open negotations. Legal and General 253k */
'0061R00001BAun6QAD', /* End date adjustment because of open negotiations. Vailt Health 69.6k */
'0061R000010seZeQAI' /* End date adjustment because of open negotiations. Corner Banca 200k */
)
)

select * from opp_ids