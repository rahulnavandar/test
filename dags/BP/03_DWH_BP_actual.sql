WITH
  IDNumber_ERP AS (
  SELECT
    PARTNER,
    IDNUMBER,
    TYPE
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but0id`
  WHERE
    TYPE = 'ZGR' ),
  IDNumber_CRM AS (
  SELECT
    PARTNER,
    IDNUMBER,
    TYPE
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but0id`
  WHERE
    TYPE = ' ZGRID'),
  SiteID_CRM AS (
  SELECT
    PARTNER,
    IDNUMBER,
    TYPE
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but0id`
  WHERE
    TYPE = ' ZGRID'),
  SiteID_ERP AS (
  SELECT
    PARTNER,
    IDNUMBER,
    TYPE
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but0id`
  WHERE
    TYPE = ' ZGRID'),
  SHOAFlag_ERP AS (
  SELECT
    DISTINCT PARTNER2
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but050`
  WHERE
    CURRENT_DATE BETWEEN DATE_FROM
    AND DATE_TO),
  SHOAFlag_CRM AS (
  SELECT
    DISTINCT PARTNER2
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but050`
  WHERE
    CURRENT_DATE BETWEEN DATE_FROM
    AND DATE_TO),
  SNHOA_ERP AS (
  SELECT
    DISTINCT PARTNER2,
    PARTNER1
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but050`
  WHERE
    CURRENT_DATE BETWEEN DATE_FROM
    AND DATE_TO
    AND RELKIND IN ('INST',
      'CONST') ),
  SNHOA_CRM AS (
  SELECT
    DISTINCT PARTNER2,
    PARTNER1
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but050`
  WHERE
    CURRENT_DATE BETWEEN DATE_FROM
    AND DATE_TO
    AND RELKIND IN ('INST',
      'CONST')),
  SNBPREL_ERP AS (
  SELECT
    DISTINCT PARTNER2,
    PARTNER1
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but050`
  WHERE
    CURRENT_DATE BETWEEN DATE_FROM
    AND DATE_TO
    AND RELKIND = "BUR011" ),
  SNBPREL_CRM AS (
  SELECT
    DISTINCT PARTNER2,
    PARTNER1
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but050`
  WHERE
    CURRENT_DATE BETWEEN DATE_FROM
    AND DATE_TO
    AND RELKIND = "BUR011"),
  BP_Transformed AS (
  SELECT
    bk.PARTNER,
    Coalesce(BUT000_ERP.TYPE,
      BUT000_CRM.type ) BW_0BP_CAT,
    Coalesce(BUT000_ERP.BPKIND,
      BUT000_CRM.BPKIND ) BW_0BP_TYPE,
    Coalesce(erp_addr.CITY1,
      crm_addr.CITY1 ) BW_0CITY,
    Coalesce(erp_addr.COUNTRY,
      crm_addr.COUNTRY ) BW_0COUNTRY,
    Coalesce(erp_addr.FAX_NUMBER,
      crm_addr.FAX_NUMBER ) BW_0FAX_NUM,
    CASE
      WHEN Coalesce(BUT000_ERP.XSEXM,  BUT000_CRM.XSEXM) = "X" AND Coalesce(BUT000_ERP.TYPE,  BUT000_CRM.TYPE) = "1" THEN "M"
      WHEN Coalesce(BUT000_ERP.XSEXF,
      BUT000_CRM.XSEXF) = "X"
    AND Coalesce(BUT000_ERP.TYPE,
      BUT000_CRM.TYPE) = "1" THEN "W"
      WHEN Coalesce(BUT000_ERP.XSEXU,  BUT000_CRM.XSEXU) = "X" AND Coalesce(BUT000_ERP.TYPE,  BUT000_CRM.TYPE) = "1" THEN "U"
    END BW_0GENDER,
    Coalesce(BUT000_ERP.LANGU_CORR,
      BUT000_CRM.LANGU_CORR) BW_0LANGU,
    UPPER(Coalesce(erp_addr.CITY2,
        crm_addr.CITY2 )) BW_0ME_DISTRCT,
    Coalesce(erp_addr.TEL_NUMBER,
      crm_addr.TEL_NUMBER ) BW_0PHONE,
    Coalesce(erp_addr.POST_CODE1,
      crm_addr.POST_CODE1 ) BW_0POSTALCODE,
    Coalesce(erp_addr.REGION,
      crm_addr.REGION ) BW_0REGION,
    UPPER(CONCAT(Coalesce(erp_addr.STREET,
          crm_addr.STREET)," ", Coalesce(erp_addr.HOUSE_NUM1,
          crm_addr.HOUSE_NUM1), " ", Coalesce(erp_addr.HOUSE_NUM2,
          crm_addr.HOUSE_NUM2))) BW_0STREET,
    Coalesce(erp_addr.SMTP_ADDR,
      crm_addr.SMTP_ADDR ) BW_OTCTEMAIL,
    CURRENT_DATETIME BW_0TCTTIMSTMP,
    Coalesce(BUT000_ERP.ADDRCOMM,
      BUT000_CRM.ADDRCOMM) BW_0SADRS_BP,
    erp_kna1.FAKSD BW_SCBILBLK,
    Coalesce(BUT000_ERP.ZZCHANNELT,
      BUT000_CRM.ZZCHANNELT) BW_SCHANNELT,
    erp_kna1.LIFSD BW_SCUDELBLK,
    CAST(CAST(Coalesce(BUT000_ERP.ZZCUSTOMERSBTYPE,
          BUT000_CRM.ZZCUSTOMERSBTYPE) AS INT64) AS STRING) BW_SCUSTSUBT,
    ID_ERP.IDNUMBER BP_SGOLREC,
    ID_CRM.IDNUMBER BP_SGRIDID,
    CASE
      WHEN zpress.ZHOA IN ("Y",  "S",  "P") THEN "Y"
      ELSE NULL
    END BW_SHOAFLAGMETAPRESS,
    CASE
      WHEN IFNULL(coalesce(SHOAFlag_ERP.PARTNER2,  SHOAFlag_CRM.PARTNER2),  "Y") = "Y" THEN "Y"
      ELSE NULL
    END BW_SHOAFLAG,
    erp_kna1.KATR1 BW_SKATR1,
    zpress.ZZMETAPRESS_ID SMETAPID,
    Coalesce(BUT000_ERP.ZZSECONDINDSEC,
      BUT000_CRM.ZZSECONDINDSEC) BW_SN2INDSC,
    UPPER(Coalesce(BUT000_ERP.NAME_LAST2,
        BUT000_CRM.NAME_LAST2)) BW_SN2LNAME,
    Coalesce(BUT000_ERP.ZZSECONDINDSEC,
      BUT000_CRM.ZZSECONDINDSEC) BW_SN3INDSC,
    Coalesce(BUT000_ERP.ZZACRONYM,
      BUT000_CRM.ZZACRONYM) BW_SNACCR,
    UPPER(CONCAT(IFNULL(Coalesce(BUT000_ERP.TITLE_ACA1,
            BUT000_CRM.TITLE_ACA1),
          ""), " ",IFNULL(Coalesce(BUT000_ERP.TITLE_ACA2,
            BUT000_CRM.TITLE_ACA2),
          ""))) BW_SNATITLE,
    Coalesce(SNBPREL_ERP.PARTNER2,
      SNBPREL_CRM.PARTNER2) SNBPREL,
    UPPER(Coalesce(BUT000_ERP.NAME_FIRST,
        BUT000_CRM.NAME_FIRST)) BW_SNFNAME,
    CASE
      WHEN zpress.ZHOA IN ("Y",  "S") THEN zpress.PARTNER
      ELSE NULL
    END BW_SNHOAMETAPRESS,
    Coalesce(SNHOA_ERP.PARTNER1,
      SNHOA_CRM.PARTNER1) BW_SNHOA,
    UPPER(CASE
        WHEN Coalesce(BUT000_ERP.BPKIND,
        BUT000_CRM.BPKIND) IN ("0001",
        "0006",
        "0007") THEN CONCAT(IFNULL(Coalesce(BUT000_ERP.NAME_LAST2,
            BUT000_CRM.NAME_LAST2),
          ""), " ",IFNULL(Coalesce(BUT000_ERP.NICKNAME,
            BUT000_CRM.NICKNAME),
          "")) END) BW_SNINAME,
    Coalesce(BUT000_ERP.IND_SECTOR,
      BUT000_CRM.IND_SECTOR) BW_SNINDSC,
    UPPER(Coalesce(BUT000_ERP.NAME_LAST,
        BUT000_CRM.NAME_LAST)) BW_SNLNAME,
    UPPER(Coalesce(BUT000_ERP.NAMEMIDDLE,
        BUT000_CRM.NAMEMIDDLE)) BW_SNMNAME,
    LTRIM(RTRIM(UPPER(CONCAT(IFNULL(Coalesce(BUT000_ERP.NAME_ORG1,
                BUT000_CRM.NAME_ORG1),
              ""), " ",IFNULL(Coalesce(BUT000_ERP.NAME_ORG2,
                BUT000_CRM.NAME_ORG2),
              ""), " ",IFNULL(Coalesce(BUT000_ERP.NAME_ORG3,
                BUT000_CRM.NAME_ORG3),
              ""), " ", IFNULL(Coalesce(BUT000_ERP.NAME_ORG4,
                BUT000_CRM.NAME_ORG4),
              ""))))) BW_SNONAME,
    Coalesce(erp_addr.PO_BOX,
      crm_addr.PO_BOX ) BW_SNPOB,
    Coalesce(erp_addr.PO_BOX_LOC,
      crm_addr.PO_BOX_LOC ) BW_SNPOBCI,
    Coalesce(erp_addr.PO_BOX_CTY,
      crm_addr.PO_BOX_CTY ) BW_SNPOBCO,
    Coalesce(erp_addr.PO_BOX_REG,
      crm_addr.PO_BOX_REG ) BW_SNPOBRE,
    Coalesce(siteid_erp.IDNUMBER,
      siteid_crm.IDNUMBER) SNSITEID,
    Coalesce(BUT000_ERP.BU_SORT1,
      BUT000_CRM.BU_SORT1) BW_SNSORT1,
    Coalesce(BUT000_ERP.BU_SORT2,
      BUT000_CRM.BU_SORT2) BW_SNSORT2,
    CAST(Coalesce(BUT000_ERP.ZZCUSTOMERTYPE,
        BUT000_CRM.ZZCUSTOMERTYPE) AS STRING) BW_SOCUSTYPE,
    IF(BUT000_CRM.PARTNER IS NULL,
      "N",
      "Y") BW_SPHYSCRM,
    IF(BUT000_CRM.PARTNER IS NOT NULL,
      IF(BUT000_ERP.PARTNER IS NOT NULL,
        "Y",
        "N"),
      "N") BW_SPHYSICAL,
    IF(BUT000_ERP.PARTNER IS NULL,
      "N",
      "Y") BW_SPHYSMPS,
    UPPER(erp_kna1.STCD1) BW_SZUMS,
    LTRIM(RTRIM(UPPER(CONCAT(BUT000_ERP.NAME_ORG1)))) as BW_SHORTNAME,
    LTRIM(RTRIM(UPPER(CONCAT(BUT000_ERP.NAME_ORG1, IFNULL(concat(" - ", BUT000_ERP.NAME_ORG2), ""))))) as BW_MEDIUMNAME,
    LTRIM(RTRIM(UPPER(CONCAT(BUT000_ERP.NAME_ORG1, IFNULL(concat(" - ", BUT000_ERP.NAME_ORG2), ""), IFNULL(concat(" - ", BUT000_ERP.NAME_ORG3), ""))))) as BW_LONGNAME
  FROM
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.BK_BusinessPartner` bk
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_but000` BUT000_ERP
  ON
    bk.PARTNER = BUT000_ERP.PARTNER
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_but000` BUT000_CRM
  ON
    bk.PARTNER = BUT000_CRM.PARTNER
  LEFT JOIN
    IDNumber_ERP ID_ERP
  ON
    bk.PARTNER = ID_ERP.PARTNER
  LEFT JOIN
    IDNumber_CRM ID_CRM
  ON
    bk.PARTNER = ID_CRM.PARTNER
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_addresses` erp_addr
  ON
    bk.PARTNER = erp_addr.PARTNER
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_crm_addresses` crm_addr
  ON
    bk.PARTNER = crm_addr.PARTNER
  LEFT JOIN
    SITEID_ERP SITEID_ERP
  ON
    bk.PARTNER = SITEID_ERP.PARTNER
  LEFT JOIN
    SITEID_CRM SITEID_CRM
  ON
    bk.PARTNER = SITEID_CRM.PARTNER
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_kna1` erp_kna1
  ON
    bk.PARTNER = erp_kna1.PARTNER
  LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.VW_BP_erp_zbutmetapress` zpress
  ON
    bk.PARTNER = zpress.PARTNER
  LEFT JOIN
    SHOAFlag_ERP
  ON
    bk.PARTNER = SHOAFlag_ERP.PARTNER2
  LEFT JOIN
    SHOAFlag_CRM
  ON
    bk.PARTNER = SHOAFlag_CRM.PARTNER2
  LEFT JOIN
    SNHOA_ERP
  ON
    bk.PARTNER = SNHOA_ERP.PARTNER2
  LEFT JOIN
    SNHOA_CRM
  ON
    bk.PARTNER = SNHOA_CRM.PARTNER2
  LEFT JOIN
    SNBPREL_ERP
  ON
    bk.PARTNER = SNBPREL_ERP.PARTNER2
  LEFT JOIN
    SNBPREL_CRM
  ON
    bk.PARTNER = SNBPREL_CRM.PARTNER2)
SELECT
  *
FROM
  BP_Transformed