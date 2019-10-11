WITH
  add_rownumber AS ( 
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY UsageCalenderDate, UsageTime, UsageMillisec,UsageSessionID, UsageContentType order by UsageCalenderDate , UsageTime , UsageMillisec  ,UsageSessionID ,UsageContentType) row_number
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_Usage.UsageDatabase_C4_{{ params.yearmonth }}` 
	where UsageCalenderDate = '{{ params.date }}')

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
  substr(UsagePlatform,1,3) as UsagePlatform,
  UsageContentType,
  UsageAccessYN,
  UsageFederatedSearch,
  1 UsageClicks,
  current_datetime() LoadedOn,
  UsageDatabase,
  UsageDatabaseId,
  'Dummy_FromDatabasePlatform' as PMkey
FROM
  add_rownumber where row_number = 1