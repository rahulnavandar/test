delete 
  `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Slink_BP_Base_Reporting_Monthly` 
WHERE 
  UsageCalenderyearmonth = '{{ params.yearmonthsep }}'