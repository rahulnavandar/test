delete 
  `{{ params.project }}.{{ params.environment }}_DWH_Usage.Usage_C4_Nature_Material_Base_Reporting_Monthly` 
WHERE 
  UsageCalenderyearmonth = '{{ params.yearmonthsep }}'