SELECT
  DATE_TRUNC(UsageCalenderDate,MONTH)UsageCalenderDate,
  UsageCalenderYear,
  UsageCalenderMonth,
  UsageCalenderYearMonth,
  UsageCalenderQuarter,
  UsagePlatform,
  UsageContentType,
  UsageAccessYN,
  UsageFederatedSearch,
  UsageDatabase,
  UsageDatabaseId,
  sum(UsageClicks) UsageClicks,
  'Dummy_FromDatabasePlatform' as PMkey
FROM
  `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Database_Material_Base_Reporting_Daily`
where
  UsageCalenderyearmonth = '{{ params.yearmonthsep }}'
GROUP BY
  DATE_TRUNC(UsageCalenderDate,MONTH),UsageCalenderYear,UsageCalenderMonth,UsageCalenderYearMonth,UsageCalenderQuarter,UsagePlatform,   UsageContentType,UsageAccessYN,UsageFederatedSearch,UsageDatabase,UsageDatabaseId,PMkey