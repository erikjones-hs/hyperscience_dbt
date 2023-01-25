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
'0061R0000135gO1QAI', /* End date adjustment because of open negotiations. Accerta 89.4k */
'0061R00000uLcG2QAK', /* End date adjustment because of open negotiations. Morgan Stanley 148.5k */ 
'0061R0000137hXuQAI', /* End date adjustment because of open negotiations. FIS Global 130k */ 
'0061R000013f0rkQAA', /* End date adjustment because of open negotiations. State of CO 214k */
'0061R000014uXZrQAM'  /* End date adjustment because of open negotiations. MPower 99k */
)
)

select * from opp_ids