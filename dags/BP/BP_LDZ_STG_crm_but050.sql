#standardSQL
INSERT INTO `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.crm_but050`   
( CLIENT, RELNR, PARTNER1, PARTNER2, DATE_TO, DATE_FROM, RELTYP, XRF, DFTVAL, RLTYP, RELKIND, CRUSR, CRDAT, CRTIM, CHUSR, CHDAT, CHTIM, XDFREL, XDFREL2, DB_KEY, DB_KEY_TD )
SELECT CLIENT, RELNR, PARTNER1, PARTNER2, DATE_TO, DATE_FROM, RELTYP, XRF, DFTVAL, RLTYP, RELKIND, CRUSR, CRDAT, CRTIM, CHUSR, CHDAT, CHTIM, XDFREL, XDFREL2, DB_KEY, DB_KEY_TD 
  FROM `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.crm_but050`   ;