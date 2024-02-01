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
'0061R00001A4rIoQAJ', /* End date adjustment because of open negotiations. Protective Life 120k */
'0061R00000yEx04QAC', /* End date adjustment because of open negotiations. HMRC / NICE 400k */
'0061R00001A4rFfQAJ', /* End date adjustment because of open negotiations. Mercury Insuarance 225k */
'0061R000010Q9dvQAC', /* End date adjustment because of open negotiations. TTC 142.5k */  
'0061R00001A4rKQQAZ'  /* Kovack. Transformed for renewal date purposes only */
)
)

select * from opp_ids