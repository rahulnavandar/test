#standardSQL
INSERT INTO `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.crm_addresses` 
( PARTNER, ADDRNUMBER, CITY1, COUNTRY, FAX_NUMBER, CITY2, TEL_NUMBER, POST_CODE1, REGION, HOUSE_NUM1, HOUSE_NUM2, STREET, SMTP_ADDR, PO_BOX, PO_BOX_LOC, PO_BOX_CTY, PO_BOX_REG )
SELECT PARTNER, ADDRNUMBER, CITY1, COUNTRY, FAX_NUMBER, CITY2, TEL_NUMBER, POST_CODE1, REGION, HOUSE_NUM1, HOUSE_NUM2, STREET, SMTP_ADDR, PO_BOX, PO_BOX_LOC, PO_BOX_CTY, PO_BOX_REG  FROM `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.crm_addresses` 