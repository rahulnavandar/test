with
  golden_ERP as ( select distinct PARTNER, IDNUMBER, TYPE, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from
  `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but0id`  WHERE TYPE = 'ZGR' ),
  golden_CRM as ( select distinct PARTNER, IDNUMBER, TYPE, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from
  `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but0id`  WHERE TYPE = 'ZGR' ),
  grid_ERP as ( select distinct PARTNER, IDNUMBER, TYPE, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from
  `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but0id`  WHERE TYPE = 'ZGRID' ),
  grid_CRM as ( select distinct PARTNER, IDNUMBER, TYPE, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from
  `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but0id`  WHERE TYPE = 'ZGRID' ),
  hoaflag_ERP as ( select distinct partner1 from `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but050`
    where current_date between DATE_FROM and DATE_TO),
  hoaflag_CRM as ( select distinct partner1 from `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but050`
    where current_date between DATE_FROM and DATE_TO),
  hoa_ERP as ( select distinct partner2 as partner, partner1  as hoa , Row_Number() over (partition by PARTNER2 order by PARTNER2 ) as RowNum from
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but050` where
    current_date between DATE_FROM and DATE_TO and RELKIND IN ('INST','CONST')),
  hoa_CRM as ( select distinct partner2 as partner, partner1 as hoa, Row_Number() over (partition by PARTNER2 order by PARTNER2 ) as RowNum from
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but050` where
    current_date between DATE_FROM and DATE_TO and RELKIND = 'BUR011'),
  bprel_ERP as ( select distinct partner1 as partner, partner2 as bprel, Row_Number() over (partition by PARTNER1 order by PARTNER1 ) as RowNum from
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but050` where
    current_date between DATE_FROM and DATE_TO and RELKIND IN ('INST','CONST')),
  bprel_CRM as ( select distinct partner1 as partner, partner2 as bprel, Row_Number() over (partition by PARTNER1 order by PARTNER1 ) as RowNum from
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but050` where
    current_date between DATE_FROM and DATE_TO and RELKIND = 'BUR011'),
  kna1_ERP as ( select distinct PARTNER, FAKSD, LIFSD, KATR1, STCD1, COUNTRY, REGION, CITY, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_kna1`),
  metapress_ERP as ( select distinct PARTNER, ZZMETAPRESS_ID, ZATHENS_CODE, ZSHIBBO_FLAG, ZMYCOPY, ZHOA, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_zbutmetapress`),
  but000_ERP as ( select distinct *, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but000`),
  but000_CRM as ( select distinct *, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but000`),
  addr_ERP as ( select distinct *, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_addresses`),
  historical_Partner as ( select *, Row_Number() over (partition by PARTNER order by PARTNER ) as RowNum from `{{ params.project }}.{{ params.environment }}_DWH_BusinessPartner.BusinessPartner_historical`),

  -- start of column selection --
  bp_transformed as (
    select
      bk.partner as PARTNER,
      coalesce( but000_ERP.TYPE, but000_CRM.TYPE, historical_Partner.BW_0BP_CAT) as BW_0BP_CAT,
      coalesce( but000_ERP.BPKIND, but000_CRM.BPKIND, historical_Partner.BW_0BP_TYPE) as BW_0BP_TYPE,
      coalesce( addr_ERP.CITY1, kna1_ERP.CITY, historical_Partner.BW_0CITY) as BW_0CITY,
      coalesce( addr_ERP.COUNTRY, kna1_ERP.COUNTRY, historical_Partner.BW_0COUNTRY) as BW_0COUNTRY,
      coalesce( addr_ERP.FAX_NUMBER, historical_Partner.BW_0FAX_NUM) as BW_0FAX_NUM,
      CASE
        WHEN Coalesce(BUT000_ERP.XSEXM, BUT000_CRM.XSEXM) = "X" AND Coalesce(BUT000_ERP.TYPE, BUT000_CRM.TYPE) = "1" THEN "M"
        WHEN Coalesce(BUT000_ERP.XSEXF, BUT000_CRM.XSEXF) = "X" AND Coalesce(BUT000_ERP.TYPE, BUT000_CRM.TYPE) = "1" THEN "W"
        WHEN Coalesce(BUT000_ERP.XSEXU,  BUT000_CRM.XSEXU) = "X" AND Coalesce(BUT000_ERP.TYPE, BUT000_CRM.TYPE) = "1" THEN "U"
      END BW_0GENDER,
      Coalesce(BUT000_ERP.LANGU_CORR, BUT000_CRM.LANGU_CORR, historical_Partner.BW_0LANGU) as BW_0LANGU,
      UPPER(Coalesce(addr_ERP.CITY2,  historical_Partner.BW_0ME_DISTRCT )) as BW_0ME_DISTRCT,
      Coalesce(addr_ERP.TEL_NUMBER, historical_Partner.BW_0PHONE ) as BW_0PHONE,
      Coalesce(addr_ERP.POST_CODE1, historical_Partner.BW_0POSTALCODE ) as BW_0POSTALCODE,
      Coalesce(addr_ERP.REGION, kna1_ERP.REGION, historical_Partner.BW_0REGION ) as BW_0REGION,
      UPPER(CONCAT(addr_ERP.STREET," ",addr_ERP.HOUSE_NUM1, " ", addr_ERP.HOUSE_NUM2)) as BW_0STREET,
      Coalesce(addr_ERP.SMTP_ADDR, historical_Partner.BW_OTCTEMAIL ) as BW_0TCTEMAIL,
      current_datetime as BW_0TCTTIMSTMP,
      Coalesce(BUT000_ERP.ADDRCOMM, historical_Partner.BW_0SADRS_BP) as BW_0SADRS_BP,
      kna1_ERP.FAKSD as BW_SCBILBLK,
      Coalesce(BUT000_ERP.ZZCHANNELT, historical_Partner.BW_SCHANNELT) as BW_SCHANNELT,
      kna1_ERP.LIFSD as BW_SCUDELEBLK,
      CAST(CAST(Coalesce(BUT000_ERP.ZZCUSTOMERSBTYPE, historical_Partner.BW_SCUSTSUBT) AS INT64) AS STRING) as BW_SCUSTSUBT,
      golden_ERP.IDNUMBER as BP_SGOLREC,
      grid_ERP.IDNUMBER as BP_SGRIDID,
      CASE
        WHEN metapress_ERP.ZHOA IN ("Y",  "S",  "P") THEN "Y"
        ELSE NULL
      END as BW_SHOAFLAGMETAPRESS,
      CASE
        WHEN metapress_ERP.ZHOA IN ("Y",  "S",  "P") THEN "Y"
        WHEN IFNULL(coalesce(hoaflag_ERP.PARTNER1,  hoaflag_CRM.PARTNER1),  "Y") = "Y" THEN null
        ELSE "Y"
      END as BW_SHOAFLAG,
      kna1_ERP.katr1 as BW_SKATR1,
      metapress_ERP.ZZMETAPRESS_ID as SMETAPID,
      Coalesce(BUT000_ERP.ZZSECONDINDSEC, BUT000_CRM.ZZSECONDINDSEC, historical_Partner.BW_SN2INDSC) as BW_SN2INDSC,
      UPPER(Coalesce(BUT000_ERP.NAME_LAST2, BUT000_CRM.NAME_LAST2, historical_Partner.BW_SN2LNAME)) as BW_SN2LNAME,
      Coalesce(BUT000_ERP.ZZSECONDINDSEC, BUT000_CRM.ZZSECONDINDSEC, historical_Partner.BW_SN3INDSC) as BW_SN3INDSC,
      Coalesce(BUT000_ERP.ZZACRONYM, BUT000_CRM.ZZACRONYM, historical_Partner.BW_SNACCR) as BW_SNACCR,
      UPPER(CONCAT(IFNULL(Coalesce(BUT000_ERP.TITLE_ACA1, BUT000_CRM.TITLE_ACA1),""), " ",IFNULL(Coalesce(BUT000_ERP.TITLE_ACA2, BUT000_CRM.TITLE_ACA2), "")))  as BW_SNATITLE,
      Coalesce(BPREL_ERP.bprel, BPREL_CRM.bprel, historical_Partner.SNBPREL) as SNBPREL,
      UPPER(Coalesce(BUT000_ERP.NAME_FIRST, BUT000_CRM.NAME_FIRST, historical_Partner.BW_SNFNAME)) as BW_SNFNAME,
      CASE
        WHEN metapress_ERP.ZHOA IN ("Y",  "S") THEN metapress_ERP.PARTNER
        ELSE historical_Partner.BW_SNHOA
      END as BW_SNHOAMETAPRESS,
      Coalesce(HOA_ERP.hoa, HOA_CRM.hoa, historical_Partner.BW_SNHOA) as BW_SNHOA,
      UPPER(CASE WHEN Coalesce(BUT000_ERP.BPKIND, BUT000_CRM.BPKIND) IN ("0001", "0006", "0007") THEN CONCAT(IFNULL(Coalesce(BUT000_ERP.NAME_LAST2, BUT000_CRM.NAME_LAST2), ""), " ",IFNULL(Coalesce(BUT000_ERP.NICKNAME, BUT000_CRM.NICKNAME), "")) END) as BW_SNINAME,
      Coalesce(BUT000_ERP.IND_SECTOR,  BUT000_CRM.IND_SECTOR, historical_Partner.BW_SNINDSC) as BW_SNINDSC,
      UPPER(Coalesce(BUT000_ERP.NAME_LAST, BUT000_CRM.NAME_LAST, historical_Partner.BW_SNLNAME)) as BW_SNLNAME,
      UPPER(Coalesce(BUT000_ERP.NAMEMIDDLE, BUT000_CRM.NAMEMIDDLE, historical_Partner.BW_SNMNAME)) as BW_SNMNAME,
      LTRIM(RTRIM(UPPER(CONCAT(IFNULL(Coalesce(BUT000_ERP.NAME_ORG1, BUT000_CRM.NAME_ORG1), ""), " ",IFNULL(Coalesce(BUT000_ERP.NAME_ORG2, BUT000_CRM.NAME_ORG2), ""), " ",IFNULL(Coalesce(BUT000_ERP.NAME_ORG3,BUT000_CRM.NAME_ORG3), ""), " ", IFNULL(Coalesce(BUT000_ERP.NAME_ORG4, BUT000_CRM.NAME_ORG4), ""))))) as BW_SNONAME,
      Coalesce(addr_ERP.PO_BOX, historical_Partner.BW_SNPOB ) as BW_SNPOB,
      Coalesce(addr_ERP.PO_BOX_LOC, historical_Partner.BW_SNPOBCI ) as BW_SNPOBCI,
      Coalesce(addr_ERP.PO_BOX_CTY, historical_Partner.BW_SNPOBCO ) as BW_SNPOBCO,
      Coalesce(addr_ERP.PO_BOX_REG, historical_Partner.BW_SNPOBRE) BW_SNPOBRE,

      '' as SNSITEID,

      Coalesce(BUT000_ERP.BU_SORT1, BUT000_CRM.BU_SORT1, historical_Partner.BW_SNSORT1) as BW_SNSORT1,
      Coalesce(BUT000_ERP.BU_SORT2, BUT000_CRM.BU_SORT2, historical_Partner.BW_SNSORT2) as BW_SNSORT2,
      CAST(Coalesce(BUT000_ERP.ZZCUSTOMERTYPE, BUT000_CRM.ZZCUSTOMERTYPE, historical_Partner.BW_SOCUSTYPE) AS STRING) as BW_SOCUSTYPE,
      IF(BUT000_CRM.PARTNER IS NULL, "N", "Y") as BW_SPHYSCRM,
      IF(BUT000_CRM.PARTNER IS NOT NULL, IF(BUT000_ERP.PARTNER IS NOT NULL,"Y","N"),"N") as BW_SPHYSICAL,
      IF(BUT000_ERP.PARTNER IS NULL,"N","Y") as BW_SPHYSMPS,
      UPPER(kna1_erp.STCD1) as BW_SZUMS,
      LTRIM(RTRIM(UPPER(CONCAT(coalesce(BUT000_ERP.NAME_ORG1, historical_Partner.BW_SHORTNAME))))) as BW_SHORTNAME,
      LTRIM(RTRIM(UPPER(CONCAT(coalesce(BUT000_ERP.NAME_ORG1, historical_Partner.BW_MEDIUMNAME), IFNULL(concat(" - ", BUT000_ERP.NAME_ORG2), ""))))) as BW_MEDIUMNAME,
      LTRIM(RTRIM(UPPER(CONCAT(coalesce(BUT000_ERP.NAME_ORG1, historical_Partner.BW_LONGNAME), IFNULL(concat(" - ", BUT000_ERP.NAME_ORG2), ""), IFNULL(concat(" - ", BUT000_ERP.NAME_ORG3), ""))))) as BW_LONGNAME,
      LTRIM(RTRIM(UPPER(CONCAT(coalesce(BUT000_ERP.NAME_ORG1, historical_Partner.BW_SNNAME), IFNULL(concat(" - ", BUT000_ERP.NAME_ORG2), ""), IFNULL(concat(" - ", BUT000_ERP.NAME_ORG3), ""))))) as BW_SNNAME
   from
      `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.BK_BusinessPartner` bk
    left join golden_ERP on bk.partner = golden_ERP.partner and golden_ERP.RowNum = 1
    left join golden_CRM on bk.partner = golden_CRM.partner and golden_CRM.RowNum = 1
    left join grid_ERP on bk.partner = grid_ERP.partner and grid_ERP.RowNum = 1
    left join grid_CRM on bk.partner = grid_CRM.partner and grid_CRM.RowNum = 1
    left join hoaflag_ERP on bk.partner = hoaflag_ERP.partner1
    left join hoaflag_CRM on bk.partner = hoaflag_CRM.partner1
    left join hoa_ERP on bk.partner = hoa_ERP.partner and hoa_ERP.RowNum = 1
    left join hoa_CRM on bk.partner = hoa_CRM.partner and hoa_CRM.RowNum = 1
    left join bprel_ERP on bk.partner = bprel_ERP.partner and bprel_ERP.RowNum = 1
    left join bprel_CRM on bk.partner = bprel_CRM.partner and bprel_CRM.RowNum = 1
    left join kna1_ERP on bk.partner = kna1_ERP.partner and kna1_ERP.RowNum = 1
    left join metapress_ERP on bk.partner = metapress_ERP.partner and metapress_ERP.RowNum = 1
    left join but000_ERP on bk.partner = but000_ERP.partner and but000_ERP.RowNum = 1
    left join but000_CRM on bk.partner = but000_CRM.partner and but000_CRM.RowNum = 1
    left join addr_ERP on bk.partner = addr_ERP.partner and addr_ERP.RowNum = 1
    left join historical_Partner on bk.partner = historical_Partner.partner
  )

  select * from bp_transformed