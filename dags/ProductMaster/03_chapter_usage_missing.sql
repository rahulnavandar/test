with
  missing_itemdoi as ( select distinct chapterDOI as chapterDOI from `{{ params.project }}.{{ params.environment }}_DWH_Usage.bk_missing_pmkeys` as a where a.doiType = 'CHAPTER' )

select usageitemdoi as chapterDOI, usageBookJournalDOI as bookDOI, usageElectronicISBN as eISBN, sum( usageclicks) as counter  FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Slink_Material_Base_Reporting_Monthly`
  where usageitemdoi in ( select chapterDOI from missing_itemdoi)
  group by chapterDOI, bookDOI, eISBN

union all
select usageitemdoi as chapterDOI, usageBookJournalDOI as bookDOI, usageElectronicISBN as eISBN, sum( usageclicks) FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Nature_Material_Base_Reporting_Monthly`
  where usageitemdoi in ( select chapterDOI from missing_itemdoi)
  group by chapterDOI, bookDOI, eISBN
union all
select usageitemdoi as chapterDOI, usageBookJournalDOI as bookDOI, usageElectronicISBN as eISBN, sum( usageclicks) FROM `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Magnus_Material_Base_Reporting_Monthly`
  where usageitemdoi in ( select chapterDOI from missing_itemdoi)
  group by chapterDOI, bookDOI, eISBN
