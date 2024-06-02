{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Merging historical data set with incrementa closed won opps */
/* This is now the full closed won opportunity data set that will be transformed */ 
with raw_data as (
select * from {{ref('arr_opp_history_1')}}
UNION 
select * from {{ref('arr_opp_history_2')}}
UNION 
select * from {{ref('arr_opp_history_3')}} 
UNION 
select * from {{ref('arr_opp_history_4')}} 
UNION 
select * from {{ref('arr_opp_history_5')}}
UNION 
select * from {{ref('arr_opp_history_6')}}
UNION 
select * from {{ref('arr_opp_history_7')}}
UNION 
select * from {{ref('arr_opp_history_8')}}
UNION 
select * from {{ref('arr_opp_history_9')}}
UNION 
select * from {{ref('arr_opp_history_10')}} 
UNION 
select * from {{ref('arr_opp_history_11')}}
UNION 
select * from {{ref('arr_opp_history_12')}}  
UNION 
select * from {{ref('arr_opp_history_13')}}
UNION 
select * from {{ref('arr_opp_history_14')}}
UNION 
select * from {{ref('arr_opp_history_15')}}
UNION 
select * from {{ref('arr_opp_history_16')}}
UNION 
select * from {{ref('arr_opp_history_17')}}
UNION 
select * from {{ref('arr_opp_history_18')}}
UNION 
select * from {{ref('arr_opp_history_19')}}
UNION
select * from {{ref('arr_opp_history_20')}}
UNION
select * from {{ref('arr_opp_history_21')}}
UNION
select * from {{ref('arr_opp_history_22')}}
UNION
select * from {{ref('arr_opp_history_23')}}
UNION
select * from {{ref('arr_opp_history_24')}}
UNION
select * from {{ref('arr_opp_history_25')}}
UNION
select * from {{ref('arr_opp_history_26')}}
UNION
select * from {{ref('arr_opp_history_27')}}
UNION
select * from {{ref('arr_opp_history_28')}}
UNION
select * from {{ref('arr_opp_history_29')}}
UNION
select * from {{ref('arr_opp_history_30')}}
UNION
select * from {{ref('arr_opp_history_31')}}
UNION
select * from {{ref('arr_opp_history_32')}}
UNION
select * from {{ref('arr_opp_history_33')}}
UNION
select * from {{ref('arr_opp_history_34')}}
UNION
select * from {{ref('arr_opp_history_35')}}
UNION
select * from {{ref('arr_opp_history_36')}}
UNION
select * from {{ref('arr_opp_history_37')}}
UNION
select * from {{ref('arr_opp_history_38')}}
UNION
select * from {{ref('arr_opp_history_39')}}
UNION
select * from {{ref('arr_opp_history_40')}}
UNION
select * from {{ref('arr_opp_history_41')}}
UNION
select * from {{ref('arr_opp_history_42')}}
UNION
select * from {{ref('arr_opp_history_43')}}
UNION
select * from {{ref('arr_opp_history_44')}}
UNION
select * from {{ref('arr_opp_history_45')}}
UNION
select * from {{ref('arr_opp_history_46')}}
UNION
select * from {{ref('arr_opp_history_47')}}
UNION
select * from {{ref('arr_opp_history_48')}}
UNION
select * from {{ref('arr_opp_history_49')}}
UNION
select * from {{ref('arr_opp_history_50')}}
UNION
select * from {{ref('arr_opp_history_51')}}
UNION
select * from {{ref('arr_opp_history_52')}}
UNION
select * from {{ref('arr_opp_history_53')}}
UNION
select * from {{ref('arr_opp_history_54')}}
UNION
select * from {{ref('arr_opp_history_55')}}
UNION
select * from {{ref('arr_opp_history_56')}}
UNION
select * from {{ref('arr_opp_history_57')}}
UNION
select * from {{ref('arr_opp_history_58')}}
UNION
select * from {{ref('arr_opp_history_59')}}
UNION
select * from {{ref('arr_opp_history_60')}}
UNION
select * from {{ref('arr_opp_history_61')}}
UNION
select * from {{ref('arr_opp_history_62')}}
UNION
select * from {{ref('arr_opp_history_63')}}
UNION
select * from {{ref('arr_opp_history_64')}}
UNION
select * from {{ref('arr_opp_history_65')}}
UNION
select * from {{ref('arr_opp_history_66')}}
UNION
select * from {{ref('arr_opp_history_67')}}
UNION
select * from {{ref('arr_opp_history_68')}}
UNION
select * from {{ref('arr_opp_history_69')}}
UNION
select * from {{ref('arr_opp_history_70')}}
UNION
select * from {{ref('arr_opp_history_71')}}
UNION
select * from {{ref('arr_opp_history_72')}}
UNION
select * from {{ref('arr_opp_history_73')}}
UNION
select * from {{ref('arr_opp_history_74')}}
UNION
select * from {{ref('arr_opp_history_75')}}
UNION
select * from {{ref('arr_opp_history_76')}}
UNION
select * from {{ref('arr_opp_history_77')}}
UNION
select * from {{ref('arr_opp_history_78')}}
UNION
select * from {{ref('arr_opp_history_79')}}
UNION
select * from {{ref('arr_opp_history_80')}}
UNION
select * from {{ref('arr_opp_history_81')}}
UNION
select * from {{ref('arr_opp_history_82')}}
UNION
select * from {{ref('arr_opp_history_83')}}
UNION
select * from {{ref('arr_opp_history_84')}}
UNION
select * from {{ref('arr_opp_history_85')}}
UNION
select * from {{ref('arr_opp_history_86')}}
UNION
select * from {{ref('arr_opp_history_87')}}
UNION
select * from {{ref('arr_opp_history_88')}}
UNION
select * from {{ref('arr_opp_history_89')}}
UNION
select * from {{ref('arr_opp_history_90')}}
UNION
select * from {{ref('arr_opp_history_91')}}
UNION
select * from {{ref('arr_opp_history_92')}}
UNION
select * from {{ref('arr_opp_history_93')}}
UNION
select * from {{ref('arr_opp_history_94')}}
UNION
select * from {{ref('arr_opp_history_95')}}
UNION
select * from {{ref('arr_opp_history_96')}}
UNION
select * from {{ref('arr_opp_history_97')}}
UNION
select * from {{ref('arr_opp_history_98')}}
UNION
select * from {{ref('arr_opp_history_99')}}
UNION
select * from {{ref('arr_opp_history_100')}}
UNION
select * from {{ref('arr_opp_history_101')}}
UNION
select * from {{ref('arr_opp_history_102')}}
UNION
select * from {{ref('arr_opp_history_103')}}
UNION
select * from {{ref('arr_opp_history_104')}}
UNION
select * from {{ref('arr_opp_history_105')}}
UNION
select * from {{ref('arr_opp_history_106')}}
order by account_id, start_dte_raw asc
)

select * from raw_data