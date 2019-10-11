create or replace table `{{ params.project }}.{{ params.environment }}_STG_Usage.Test_PM_Aggr`
as
select distinct u.UsageItemDOI, u.UsageBookJournalDOI,   u.UsageJournalNo ,  u.UsagePublisherType, 
concat(u.UsageItemDOI, "||" ,u.UsageBookJournalDOI,   "||" ,u.UsageJournalNo , "||" ,u.UsagePublisherType)
PMKey  from
`{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_BP_Base_Reporting_MonthlyV2` u
UNION DISTINCT 
select distinct u.UsageItemDOI, u.UsageBookJournalDOI,   u.UsageJournalNo ,  u.UsagePublisherType, 
concat(u.UsageItemDOI, "||" ,u.UsageBookJournalDOI,   "||" ,u.UsageJournalNo , "||" ,u.UsagePublisherType)
PMKey  from
`{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Magnus_BP_Base_Reporting_MonthlyV2` u
UNION DISTINCT
select distinct u.UsageItemDOI, u.UsageBookJournalDOI,   u.UsageJournalNo ,  u.UsagePublisherType, 
concat(u.UsageItemDOI, "||" ,u.UsageBookJournalDOI,   "||" ,u.UsageJournalNo , "||" ,u.UsagePublisherType)
PMKey  from
`{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Nature_BP_Base_Reporting_MonthlyV2` u 