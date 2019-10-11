#standardSQL
INSERT INTO `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.erp_kna1`     
( PARTNER, FAKSD, LIFSD, KATR1, STCD1 )
SELECT PARTNER, FAKSD, LIFSD, KATR1, STCD1 
  FROM `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.erp_kna1`     ;