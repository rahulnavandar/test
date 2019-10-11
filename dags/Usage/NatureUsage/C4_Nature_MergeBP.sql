MERGE `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.BP_Usage`  T
USING 
(
SELECT
  UsageBusinessPartner,
  UsageBusinessPartnerType,
  Count(*) Counter
  FROM
  `{{ params.project }}.{{ params.environment }}_STG_Usage.UsageNature_C4_{{ params.yearmonth }}` usage
  where UsageCalenderDate = '{{ params.date }}'
  GROUP BY UsageBusinessPartner,
  UsageBusinessPartnerType
) S
ON 
T.UsageBusinessPartner = S.UsageBusinessPartner 
AND T.UsageBusinessPartnerType = S.UsageBusinessPartnerType
WHEN NOT MATCHED THEN
  INSERT(UsageBusinessPartner,
  UsageBusinessPartnerType,
  Counter,
  UpdatedOn)
  VALUES(S.UsageBusinessPartner,
  S.UsageBusinessPartnerType,
  S.Counter,
  CURRENT_DATETIME())
WHEN MATCHED THEN
  UPDATE SET 
  Counter = T.Counter + S.Counter,
  UpdatedOn = CURRENT_DATETIME()