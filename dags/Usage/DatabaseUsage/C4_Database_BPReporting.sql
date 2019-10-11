SELECT
  UsageCalenderDate,
  UsageCalenderDateTime,
  UsageCalenderDateFile,
  UsageMillisec,
  UsageCalenderYear,
  UsageCalenderMonth,
  UsageCalenderYearMonth,
  UsageCalenderQuarter,
  UsageTime,
  UsageTimeFull,
  UsageSessionID,
  UsageClientIP,
  UsageClientIPSearch,
  UsageBusinessPartner,
  substr(UsagePlatform,1,3) as UsagePlatform,
  UsageContentType,
  UsageAccessYN,
  UsageFederatedSearch,
  UsageClicks,
  CURRENT_DATETIME LoadedOn,
  UsageDatabase,
  UsageDatabaseId,
  'Dummy_FromDatabasePlatform' as PMkey
FROM
  `{{ params.project }}.{{ params.environment }}_STG_Usage.UsageDatabase_C4_{{ params.yearmonth }}`
where UsageCalenderDate = '{{ params.date }}'