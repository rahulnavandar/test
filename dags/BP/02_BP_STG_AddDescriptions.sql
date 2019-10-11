WITH
  bptype AS (
  SELECT
    CASE
      WHEN LENGTH(BPType) = 1 THEN CONCAT("000",BPType)
      ELSE BPType
    END BPType,
    Description
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.text_BPType`)
SELECT
  bp.*,
  CASE
    WHEN bp.BW_SNONAME IS NOT NULL AND LENGTH(bp.BW_SNONAME) > 0 THEN bp.BW_SNONAME
    WHEN bp.BW_SNINAME IS NOT NULL
  AND LENGTH(LTRIM(RTRIM(bp.BW_SNINAME))) > 0 THEN bp.BW_SNINAME
    WHEN bp.BW_0BP_TYPE IN ("0001",  "0006",  "0007",  "JOUR",  "MA") AND LENGTH(LTRIM(RTRIM(bp.BW_SNINAME))) = 0 AND LENGTH(bp.BW_SNONAME) = 0 THEN RTRIM(TRIM(CONCAT( ifnull(bp.BW_SNATITLE,  "")," ",ifnull(bp.BW_SNFNAME,  "")," ",ifnull(bp.BW_SNMNAME,  "")," ",ifnull(bp.BW_SNLNAME,  ""))))
    ELSE "No name found"
  END BW_SNNAME,
  c.Description CustomerTypeDesc,
  ct.Description ChannelTypeDesc,
  cst.Description CustomerSubTypeDesc,
  co.description CountryDesc,
  re.description RegionDesc,
  hoaflagmetapress.Description HOAFLAGMetapressDesc,
  hoaflag.Description HOAFLAGDesc,
  bpt.Description BPDesc,
  cat.Description BPCatDesc
FROM
  `{{ params.project }}.{{ params.environment }}_STG_BusinessPartner.BusinessPartner_Step1`  bp
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.text_CustomerType` c
ON
  SAFE_CAST(SAFE_CAST(bp.BW_SOCUSTYPE AS INT64) AS STRING) = c.CustomerType
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.text_ChannelType` ct
ON
  bp.BW_SCHANNELT = ct.Channel
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.text_CustomerSubType`cst
ON
  SAFE_CAST(SAFE_CAST(bp.BW_SCUSTSUBT AS INT64) AS STRING) = cst.CustomerSub
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.texts_country` co
ON
  bp.BW_0COUNTRY = co.country
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.texts_region` re
ON
  bp.bw_0REGION = re.region
  AND bp.BW_0COUNTRY = re.country
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.texts_hoaflag` hoaflag
ON
  bp.BW_SHOAFLAG = hoaflag.HoAFlag
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.texts_hoaflag` hoaflagmetapress
ON
  bp.BW_SHOAFLAG = hoaflagmetapress.HoAFlag
LEFT JOIN
  bptype bpt
ON
  bp.BW_0BP_TYPE = bpt.BPType
LEFT JOIN
  `{{ params.project }}.{{ params.environment }}_LDZ_BusinessPartner.texts_category` cat
ON
  bp.BW_0BP_CAT = cat.Category