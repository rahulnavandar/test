  SELECT
    DISTINCT(upper(replace(CONCAT("EM_", em.ID), "-", "/"))) as SubmissionKey
  , max(REVISION) LatestRevision
  , ms_id ManuscriptID
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.em_submissions` em
  GROUP BY upper(replace(CONCAT("EM_", em.ID), "-", "/")) , ms_id
  UNION DISTINCT
  SELECT
    DISTINCT(upper(CONCAT("EJP_", upper(ejp_map.ejp_j_abbrev),"/", cast(ejp.ms_id as string)))) as SubmissionKey
  , max(MS_REV_NO) LatestRevision
  , (select CMPT_MS_NM from `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.ejp_submissions` where dbname = ejp.dbname and ms_id = ejp.ms_id and ms_rev_no = 0) ManuscriptID
  FROM
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.ejp_submissions` ejp
  LEFT JOIN `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_mapping_ejp` ejp_map
    ON ejp.j_id = ejp_map.ejp_j_id
  GROUP BY upper(CONCAT("EJP_", upper(ejp_map.ejp_j_abbrev),"/", cast(ejp.ms_id as string))) , ManuscriptID
