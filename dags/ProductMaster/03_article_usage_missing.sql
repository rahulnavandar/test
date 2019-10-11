with
  missing_itemdoi as ( select distinct articleDOI as articleDOI from `{{ params.project }}.{{ params.environment }}_DWH_Usage.bk_missing_pmkeys` as a where a.doiType = 'ARTICLE' )

select usageitemdoi as articleDOI, usageBookJournalDOI as journalDOI, usageElectronicISSN as eISSN, UsageJournalNo  as JournalTitle, sum( usageclicks) as counter  FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Slink_Material_Base_Reporting_Monthly`
  where usageitemdoi in ( select articleDOI from missing_itemdoi)
  group by articleDOI, journalDOI, eISSN, UsageJournalNo

union all
select usageitemdoi as articleDOI, usageBookJournalDOI as journalDOI, usageElectronicISSN as eISSN, UsageJournalNo  as JournalTitle, sum( usageclicks) FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Nature_Material_Base_Reporting_Monthly`
  where usageitemdoi in ( select articleDOI from missing_itemdoi)
  group by articleDOI, journalDOI, eISSN, UsageJournalNo
union all
select usageitemdoi as articleDOI, usageBookJournalDOI as journalDOI, usageElectronicISSN as eISSN, UsageJournalNo  as JournalTitle, sum( usageclicks) FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Magnus_Material_Base_Reporting_Monthly`
  where usageitemdoi in ( select articleDOI from missing_itemdoi)
  group by articleDOI, journalDOI, eISSN, UsageJournalNo
