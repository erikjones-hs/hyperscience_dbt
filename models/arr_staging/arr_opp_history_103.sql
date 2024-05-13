{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'ARR_STAGING'
)
}}

with raw_data_hist as (
select * from {{ref('arr_opp_history_1')}}
),

raw_data_inc as (
select * from {{ref('arr_opp_history_2')}}    
),

raw_data_inc_2 as (
select * from {{ref('arr_opp_history_3')}}    
),

raw_data_inc_3 as (
select * from {{ref('arr_opp_history_4')}}    
),

raw_data_inc_4 as (
select * from {{ref('arr_opp_history_5')}}    
),

raw_data_inc_5 as (
select * from {{ref('arr_opp_history_6')}}    
),

raw_data_inc_6 as (
select * from {{ref('arr_opp_history_7')}}    
),

raw_data_inc_7 as (
select * from {{ref('arr_opp_history_8')}}    
),

raw_data_inc_8 as (
select * from {{ref('arr_opp_history_9')}}    
),

raw_data_inc_9 as (
select * from {{ref('arr_opp_history_10')}}    
),

raw_data_inc_10 as (
select * from {{ref('arr_opp_history_11')}}    
),

raw_data_inc_11 as (
select * from {{ref('arr_opp_history_12')}}    
),

raw_data_inc_12 as (
select * from {{ref('arr_opp_history_13')}}    
),

raw_data_inc_13 as (
select * from {{ref('arr_opp_history_14')}}    
),

raw_data_inc_14 as (
select * from {{ref('arr_opp_history_15')}}    
),

raw_data_inc_15 as (
select * from {{ref('arr_opp_history_16')}}    
),

raw_data_inc_16 as (
select * from {{ref('arr_opp_history_17')}}    
),

raw_data_inc_17 as (
select * from {{ref('arr_opp_history_18')}}    
),

raw_data_inc_18 as (
select * from {{ref('arr_opp_history_19')}}    
),

raw_data_inc_19 as (
select * from {{ref('arr_opp_history_20')}}    
),

raw_data_inc_20 as (
select * from {{ref('arr_opp_history_21')}}    
),

raw_data_inc_21 as (
select * from {{ref('arr_opp_history_22')}}    
),

raw_data_inc_22 as (
select * from {{ref('arr_opp_history_23')}}    
),

raw_data_inc_23 as (
select * from {{ref('arr_opp_history_24')}}    
),

raw_data_inc_24 as (
select * from {{ref('arr_opp_history_25')}}    
),

raw_data_inc_25 as (
select * from {{ref('arr_opp_history_26')}}    
),

raw_data_inc_26 as (
select * from {{ref('arr_opp_history_27')}}    
),

raw_data_inc_27 as (
select * from {{ref('arr_opp_history_28')}}    
),

raw_data_inc_28 as (
select * from {{ref('arr_opp_history_29')}}    
),

raw_data_inc_29 as (
select * from {{ref('arr_opp_history_30')}}    
),

raw_data_inc_30 as (
select * from {{ref('arr_opp_history_31')}}    
),

raw_data_inc_31 as (
select * from {{ref('arr_opp_history_32')}}    
),

raw_data_inc_32 as (
select * from {{ref('arr_opp_history_33')}}    
),

raw_data_inc_33 as (
select * from {{ref('arr_opp_history_34')}}    
),

raw_data_inc_34 as (
select * from {{ref('arr_opp_history_35')}}    
),

raw_data_inc_35 as (
select * from {{ref('arr_opp_history_36')}}    
),

raw_data_inc_36 as (
select * from {{ref('arr_opp_history_37')}}    
),

raw_data_inc_37 as (
select * from {{ref('arr_opp_history_38')}}    
),

raw_data_inc_38 as (
select * from {{ref('arr_opp_history_39')}}    
),

raw_data_inc_39 as (
select * from {{ref('arr_opp_history_40')}}    
),

raw_data_inc_40 as (
select * from {{ref('arr_opp_history_41')}}    
),

raw_data_inc_41 as (
select * from {{ref('arr_opp_history_42')}}    
),

raw_data_inc_42 as (
select * from {{ref('arr_opp_history_43')}}    
),

raw_data_inc_43 as (
select * from {{ref('arr_opp_history_44')}}    
),

raw_data_inc_44 as (
select * from {{ref('arr_opp_history_45')}}    
),

raw_data_inc_45 as (
select * from {{ref('arr_opp_history_46')}}    
),

raw_data_inc_46 as (
select * from {{ref('arr_opp_history_47')}}    
),

raw_data_inc_47 as (
select * from {{ref('arr_opp_history_48')}}    
),

raw_data_inc_48 as (
select * from {{ref('arr_opp_history_49')}}    
),

raw_data_inc_49 as (
select * from {{ref('arr_opp_history_50')}}    
),

raw_data_inc_50 as (
select * from {{ref('arr_opp_history_51')}}    
),

raw_data_inc_51 as (
select * from {{ref('arr_opp_history_52')}}    
),

raw_data_inc_52 as (
select * from {{ref('arr_opp_history_53')}}    
),

raw_data_inc_53 as (
select * from {{ref('arr_opp_history_54')}}    
),

raw_data_inc_54 as (
select * from {{ref('arr_opp_history_55')}}    
),

raw_data_inc_55 as (
select * from {{ref('arr_opp_history_56')}}    
),

raw_data_inc_56 as (
select * from {{ref('arr_opp_history_57')}}    
),

raw_data_inc_57 as (
select * from {{ref('arr_opp_history_58')}}    
),

raw_data_inc_58 as (
select * from {{ref('arr_opp_history_59')}}    
),

raw_data_inc_59 as (
select * from {{ref('arr_opp_history_60')}}    
),

raw_data_inc_60 as (
select * from {{ref('arr_opp_history_61')}}    
),

raw_data_inc_61 as (
select * from {{ref('arr_opp_history_62')}}    
),

raw_data_inc_62 as (
select * from {{ref('arr_opp_history_63')}}    
),

raw_data_inc_63 as (
select * from {{ref('arr_opp_history_64')}}    
),

raw_data_inc_64 as (
select * from {{ref('arr_opp_history_65')}}    
),

raw_data_inc_65 as (
select * from {{ref('arr_opp_history_66')}}    
),

raw_data_inc_66 as (
select * from {{ref('arr_opp_history_67')}}    
),

raw_data_inc_67 as (
select * from {{ref('arr_opp_history_68')}}    
),

raw_data_inc_68 as (
select * from {{ref('arr_opp_history_69')}}    
),

raw_data_inc_69 as (
select * from {{ref('arr_opp_history_70')}}    
),

raw_data_inc_70 as (
select * from {{ref('arr_opp_history_71')}}    
),

raw_data_inc_71 as (
select * from {{ref('arr_opp_history_72')}}    
),

raw_data_inc_72 as (
select * from {{ref('arr_opp_history_73')}}    
),

raw_data_inc_73 as (
select * from {{ref('arr_opp_history_74')}}    
),

raw_data_inc_74 as (
select * from {{ref('arr_opp_history_75')}}    
),

raw_data_inc_75 as (
select * from {{ref('arr_opp_history_76')}}    
),

raw_data_inc_76 as (
select * from {{ref('arr_opp_history_77')}}    
),

raw_data_inc_77 as (
select * from {{ref('arr_opp_history_78')}}    
),

raw_data_inc_78 as (
select * from {{ref('arr_opp_history_79')}}    
),

raw_data_inc_79 as (
select * from {{ref('arr_opp_history_80')}}    
),

raw_data_inc_80 as (
select * from {{ref('arr_opp_history_81')}}    
),

raw_data_inc_81 as (
select * from {{ref('arr_opp_history_82')}}    
),

raw_data_inc_82 as (
select * from {{ref('arr_opp_history_83')}}    
),

raw_data_inc_83 as (
select * from {{ref('arr_opp_history_84')}}    
),

raw_data_inc_84 as (
select * from {{ref('arr_opp_history_85')}}    
),

raw_data_inc_85 as (
select * from {{ref('arr_opp_history_86')}}    
),

raw_data_inc_86 as (
select * from {{ref('arr_opp_history_87')}}    
),

raw_data_inc_87 as (
select * from {{ref('arr_opp_history_88')}}    
),

raw_data_inc_88 as (
select * from {{ref('arr_opp_history_89')}}    
),

raw_data_inc_89 as (
select * from {{ref('arr_opp_history_90')}}    
),

raw_data_inc_90 as (
select * from {{ref('arr_opp_history_91')}}    
),

raw_data_inc_91 as (
select * from {{ref('arr_opp_history_92')}}    
),

raw_data_inc_92 as (
select * from {{ref('arr_opp_history_93')}}    
),

raw_data_inc_93 as (
select * from {{ref('arr_opp_history_94')}}    
),

raw_data_inc_94 as (
select * from {{ref('arr_opp_history_95')}}    
),

raw_data_inc_95 as (
select * from {{ref('arr_opp_history_96')}}    
),

raw_data_inc_96 as (
select * from {{ref('arr_opp_history_97')}}    
),

raw_data_inc_97 as (
select * from {{ref('arr_opp_history_98')}}    
),

raw_data_inc_98 as (
select * from {{ref('arr_opp_history_99')}}    
),

raw_data_inc_99 as (
select * from {{ref('arr_opp_history_100')}}    
),

raw_data_inc_100 as (
select * from {{ref('arr_opp_history_101')}}    
),

raw_data_inc_101 as (
select * from {{ref('arr_opp_history_102')}}    
),

raw_data_inc_102 as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
opp_is_marketing_influenced_flag,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
to_date(opp_closed_won_dte) as closed_won_dte,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v102"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_id not in (select opp_id from raw_data_inc_2) 
and opp_id not in (select opp_id from raw_data_inc_3)
and opp_id not in (select opp_id from raw_data_inc_4) 
and opp_id not in (select opp_id from raw_data_inc_5) 
and opp_id not in (select opp_id from raw_data_inc_6)
and opp_id not in (select opp_id from raw_data_inc_7) 
and opp_id not in (select opp_id from raw_data_inc_8)
and opp_id not in (select opp_id from raw_data_inc_9)  
and opp_id not in (select opp_id from raw_data_inc_10)
and opp_id not in (select opp_id from raw_data_inc_11)  
and opp_id not in (select opp_id from raw_data_inc_12)
and opp_id not in (select opp_id from raw_data_inc_13)
and opp_id not in (select opp_id from raw_data_inc_14)
and opp_id not in (select opp_id from raw_data_inc_15)
and opp_id not in (select opp_id from raw_data_inc_16)
and opp_id not in (select opp_id from raw_data_inc_17)
and opp_id not in (select opp_id from raw_data_inc_18)
and opp_id not in (select opp_id from raw_data_inc_19)
and opp_id not in (select opp_id from raw_data_inc_20)
and opp_id not in (select opp_id from raw_data_inc_21)
and opp_id not in (select opp_id from raw_data_inc_22)
and opp_id not in (select opp_id from raw_data_inc_23)
and opp_id not in (select opp_id from raw_data_inc_24)
and opp_id not in (select opp_id from raw_data_inc_25)
and opp_id not in (select opp_id from raw_data_inc_26)
and opp_id not in (select opp_id from raw_data_inc_27)
and opp_id not in (select opp_id from raw_data_inc_28)
and opp_id not in (select opp_id from raw_data_inc_29)
and opp_id not in (select opp_id from raw_data_inc_30)
and opp_id not in (select opp_id from raw_data_inc_31)
and opp_id not in (select opp_id from raw_data_inc_32)
and opp_id not in (select opp_id from raw_data_inc_33)
and opp_id not in (select opp_id from raw_data_inc_34)
and opp_id not in (select opp_id from raw_data_inc_35)
and opp_id not in (select opp_id from raw_data_inc_36)
and opp_id not in (select opp_id from raw_data_inc_37)
and opp_id not in (select opp_id from raw_data_inc_38)
and opp_id not in (select opp_id from raw_data_inc_39)
and opp_id not in (select opp_id from raw_data_inc_40)
and opp_id not in (select opp_id from raw_data_inc_41)
and opp_id not in (select opp_id from raw_data_inc_42)
and opp_id not in (select opp_id from raw_data_inc_43)
and opp_id not in (select opp_id from raw_data_inc_44)
and opp_id not in (select opp_id from raw_data_inc_45)
and opp_id not in (select opp_id from raw_data_inc_46)
and opp_id not in (select opp_id from raw_data_inc_47)
and opp_id not in (select opp_id from raw_data_inc_48)
and opp_id not in (select opp_id from raw_data_inc_49)
and opp_id not in (select opp_id from raw_data_inc_50)
and opp_id not in (select opp_id from raw_data_inc_51)
and opp_id not in (select opp_id from raw_data_inc_52)
and opp_id not in (select opp_id from raw_data_inc_53)
and opp_id not in (select opp_id from raw_data_inc_54)
and opp_id not in (select opp_id from raw_data_inc_55)
and opp_id not in (select opp_id from raw_data_inc_56)
and opp_id not in (select opp_id from raw_data_inc_57)
and opp_id not in (select opp_id from raw_data_inc_58)
and opp_id not in (select opp_id from raw_data_inc_59)
and opp_id not in (select opp_id from raw_data_inc_60)
and opp_id not in (select opp_id from raw_data_inc_61)
and opp_id not in (select opp_id from raw_data_inc_62)
and opp_id not in (select opp_id from raw_data_inc_63)
and opp_id not in (select opp_id from raw_data_inc_64)
and opp_id not in (select opp_id from raw_data_inc_65)
and opp_id not in (select opp_id from raw_data_inc_66)
and opp_id not in (select opp_id from raw_data_inc_67)
and opp_id not in (select opp_id from raw_data_inc_68)
and opp_id not in (select opp_id from raw_data_inc_69)
and opp_id not in (select opp_id from raw_data_inc_70)
and opp_id not in (select opp_id from raw_data_inc_71)
and opp_id not in (select opp_id from raw_data_inc_72)
and opp_id not in (select opp_id from raw_data_inc_73)
and opp_id not in (select opp_id from raw_data_inc_74)
and opp_id not in (select opp_id from raw_data_inc_75)
and opp_id not in (select opp_id from raw_data_inc_76)
and opp_id not in (select opp_id from raw_data_inc_77)
and opp_id not in (select opp_id from raw_data_inc_78)
and opp_id not in (select opp_id from raw_data_inc_79)
and opp_id not in (select opp_id from raw_data_inc_80)
and opp_id not in (select opp_id from raw_data_inc_81)
and opp_id not in (select opp_id from raw_data_inc_82)
and opp_id not in (select opp_id from raw_data_inc_83)
and opp_id not in (select opp_id from raw_data_inc_84)
and opp_id not in (select opp_id from raw_data_inc_85)
and opp_id not in (select opp_id from raw_data_inc_86)
and opp_id not in (select opp_id from raw_data_inc_87)
and opp_id not in (select opp_id from raw_data_inc_88)
and opp_id not in (select opp_id from raw_data_inc_89)
and opp_id not in (select opp_id from raw_data_inc_90)
and opp_id not in (select opp_id from raw_data_inc_91)
and opp_id not in (select opp_id from raw_data_inc_92)
and opp_id not in (select opp_id from raw_data_inc_93)
and opp_id not in (select opp_id from raw_data_inc_94)
and opp_id not in (select opp_id from raw_data_inc_95)
and opp_id not in (select opp_id from raw_data_inc_96)
and opp_id not in (select opp_id from raw_data_inc_97)
and opp_id not in (select opp_id from raw_data_inc_98)
and opp_id not in (select opp_id from raw_data_inc_99)
and opp_id not in (select opp_id from raw_data_inc_100)
and opp_id not in (select opp_id from raw_data_inc_101)
and opp_arr > 0
and opp_id not in ('006Pm00000CXKYvIAP','0061R000016nuiyQAA') /* Removing a test opportunity */
order by account_id, start_dte_raw asc
)

select * from raw_data_inc_102