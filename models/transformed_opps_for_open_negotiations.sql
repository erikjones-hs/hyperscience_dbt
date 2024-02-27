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
'0061R00000uL8ylQAC', /* End date adjustment because of open negitotaions. PMP $0 */
'0061R00000yFonNQAS', /* ENd date adjustment because of open negotiations. Metasource $0 */   
'0061R000010Q9dvQAC', /* End date adjustment because of open negotiations. TTC 142.5k */  
'0061R00001A4rKQQAZ', /* Kovack. Transformed for renewal date purposes only */
'0061R00000yGqH3QAK', /* End date adjustment because of extension. SSA 2.3M */
'0061R00001A5k8bQAB', /* End date adjustment because of extension. SSA 1.45M */
'0061R000014wNrtQAE', /* End date adjustment because of extension. SSA 1.93M */
'0061R000019QwEZQA0', /* End date adjustment because of open negotiations. DLA 40k */
'0061R00000zD2sxQAC'  /* End date adjustment because of open negotiations. Coduent 1.98M */
)
)

select * from opp_ids