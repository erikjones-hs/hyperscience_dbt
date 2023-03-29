{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'GONG'
)
}}

with calls as (
select distinct 
c.WORKSPACE_ID,
c.CONVERSATION_ID as call_id,
c.CONVERSATION_KEY,
cl.OWNER_ID,
cl.title,
cl.EFFECTIVE_START_DATETIME::date as call_date,
cr.start_datetime as call_start,
cr.end_timetime as call_end,
timestampdiff(minute,call_start,call_end) as call_duration,
u.user_id,
u.email_address,
u.first_name,
u.last_name,
uci.crm_user_id as crm_id
from GONG.HYPERSCIENCE_GONG.CONVERSATIONS as c 
join GONG.HYPERSCIENCE_GONG.CALLS as cl on (c.CONVERSATION_KEY = cl.CONVERSATION_KEY)
join GONG.HYPERSCIENCE_GONG.CALL_RECORDINGS as cr on (c.conversation_key = cr.conversation_key)
left join GONG.HYPERSCIENCE_GONG.USERS u on (cl.OWNER_ID = u.USER_ID)
left join "GONG"."HYPERSCIENCE_GONG"."USER_CRM_IDS" as uci on (u.user_id = uci.user_id)
where cl.STATUS = 'COMPLETED' 
and c.CONVERSATION_TYPE = 'call' 
)

select * from calls