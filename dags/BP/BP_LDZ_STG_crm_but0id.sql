#standardSQL
INSERT INTO `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.crm_but0id`    
( CLIENT, PARTNER, TYPE, IDNUMBER, INSTITUTE, ENTRY_DATE, VALID_DATE_FROM, VALID_DATE_TO, COUNTRY, REGION, IDNUMBER_GUID, BP_EEW_BUT0ID )
SELECT CLIENT, PARTNER, TYPE, IDNUMBER, INSTITUTE, ENTRY_DATE, VALID_DATE_FROM, VALID_DATE_TO, COUNTRY, REGION, IDNUMBER_GUID, BP_EEW_BUT0ID 
  FROM `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.crm_but0id`    ;