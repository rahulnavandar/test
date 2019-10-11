#standardSQL
INSERT INTO `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.erp_zbutmetapress`      
( PARTNER, ZZMETAPRESS_ID, ZATHENS_CODE, ZSHIBBO_FLAG, ZMYCOPY, ZHOA )
SELECT PARTNER, ZZMETAPRESS_ID, ZATHENS_CODE, ZSHIBBO_FLAG, ZMYCOPY, ZHOA
  FROM `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.erp_zbutmetapress`;