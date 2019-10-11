MERGE `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.BP_Usage`  T
USING 
(
SELECT
  UsageBusinessPartner,
  UsageBusinessPartnerType,
  UsageBPName,
  Count(*) Counter
  FROM
  `{{ params.project }}.{{ params.environment }}_STG_Usage.Usage_C4_{{ yesterday_ds_nodash }}` usage
  GROUP BY UsageBusinessPartner,
  UsageBusinessPartnerType,
  UsageBPName
) S
ON 
T.UsageBusinessPartner = S.UsageBusinessPartner 
AND T.UsageBusinessPartnerType = S.UsageBusinessPartnerType
AND T.UsageBPName = S.UsageBPName
WHEN NOT MATCHED THEN
  INSERT(UsageBusinessPartner,
  UsageBusinessPartnerType,
  UsageBPName,
  Counter,
  UpdatedOn)
  VALUES(S.UsageBusinessPartner,
  S.UsageBusinessPartnerType,
  S.UsageBPName,
  S.Counter,
  CURRENT_DATETIME())
WHEN MATCHED THEN
  UPDATE SET 
  Counter = T.Counter + S.Counter,
  UpdatedOn = CURRENT_DATETIME()