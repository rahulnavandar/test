WITH
transfers as (select distinct from_j_id, from_ms_id, from_ms_rev_no, trans.j_id,trans.ms_id,trans.ms_rev_no,transfer_dt,ejp_map.ejp_j_abbrev AbbrevTo,ejp_map2.ejp_j_abbrev AbbrevFrom,
CONCAT("EJP_", upper(ejp_map.ejp_j_abbrev),"/", cast(trans.ms_id as string)) TransferredTo
,CONCAT("EJP_", upper(ejp_map2.ejp_j_abbrev),"/", cast(trans.from_ms_id as string)) TransferredFrom
from `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.ejp_transfers` trans
left join `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_mapping_ejp` ejp_map
ON trans.j_id = ejp_map.ejp_j_id
left join `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_mapping_ejp` ejp_map2
ON trans.from_j_id = ejp_map2.ejp_j_id),
transfers2 as (select distinct from_j_id, from_ms_id, from_ms_rev_no, trans.j_id,trans.ms_id,trans.ms_rev_no,transfer_dt,ejp_map.ejp_j_abbrev AbbrevTo,ejp_map2.ejp_j_abbrev AbbrevFrom,
CONCAT("EJP_", upper(ejp_map.ejp_j_abbrev),"/", cast(trans.ms_id as string)) TransferredTo
,CONCAT("EJP_", upper(ejp_map2.ejp_j_abbrev),"/", cast(trans.from_ms_id as string)) TransferredFrom
from `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.ejp_transfers` trans
left join `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_mapping_ejp` ejp_map
ON trans.j_id = ejp_map.ejp_j_id
left join `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_mapping_ejp` ejp_map2
ON trans.from_j_id = ejp_map2.ejp_j_id),
  getsubmissionkey AS (
SELECT
    SubmissionKey, LatestRevision, ManuscriptID
FROM
    `{{ params.project }}.{{ params.environment }}_STG_ProductMaster.BK_SubmissionKey`),
  getcolumns AS (
SELECT
    bk.SubmissionKey,
    coalesce(em.doc_id,
             ejp.ms_id) DocumentID,
    coalesce(concat(em.ms_id, ".", cast(em.revision as string)),
             bk.ManuscriptID) ManuscriptNumber,
--             concat(ejp.cmpt_ms_nm, case when ejp.ms_rev_no = 0 then "" else concat(".", cast(ejp.ms_rev_no as string)) end )) ManuscriptNumber,
    upper(coalesce(em.ms_id,
                   bk.ManuscriptID)) ManuscriptID,
--                   ejp.cmpt_ms_nm)) ManuscriptID,
    coalesce(em.revision,
             ejp.ms_rev_no) Revision,
    coalesce(jfw.journal_key,
             ejp_map.sjsnr) JournalTitleNumber,
    -- Begin Add eJP Manuscript Type Text here
    em.article_type ArticleType,
    -- End Add eJP Manuscript Type Text here
    concat(upper(ejp.dbname),"/",cast(ejp.ms_type_cde as string)) ManuscriptType,
    coalesce(replace(substr(em.initial_subm_dat,1,10), ".", "-"),
             substr(presubm.subm_auth_app_dt,1,10) )Initialized,
    coalesce(replace(substr(em.first_receipt_dat,1,10), ".", "-"),
             replace(substr(presubm.subm_appr_dt,1,10), ".","-") ) FirstReceiptDate,
    coalesce(em.title,
             case when ejp.ms_title = 'Unknown Title' then ''
             else ejp.ms_title
             end) Title,
    coalesce(em.status,
             cast(ejp.current_stage_id as string) ) CurrentStatus,
--    stage.stage_nm CurrentStatusText,
    coalesce(replace(substr(em.status_dat,1,10), ".", "-"),
             replace(substr(ejp.current_stage_dt,1,10), ".", "-")) StatusDate,
    em.final_dec_fam FinalDecisionFamily,
    replace(substr(em.final_dec_dat,1,10), ".", "-") FinalDecisionDate,
    coalesce(em.final_disp_term,
             case when ejp.FINAL_DECISION_IND = 1 then 'ACCEPT'
                  when ejp.FINAL_DECISION_IND = 2 then 'MINOR REVISION'
                  when ejp.FINAL_DECISION_IND = 3 then 'MAJOR REVISION'
                  when ejp.FINAL_DECISION_IND = 4 then 'REJECT'
                  when ejp.FINAL_DECISION_IND = 6 then 'WITHDRAWN'
                  else ''
             end) FinalDisposition,
    coalesce(replace(substr(em.final_disp_dat,1,10), ".", "-"),
             replace(substr(ejp.final_decision_dt,1,10),".","-")) FinalDispositionDate,
    upper(cor_au_ctry_iso) CountryKey,
    1 NumberOfSubmissions,
    replace(cast(em.TRANS_STAT_REC as string), " ", "0") TransferStatusReceived,
    coalesce(em.PRODUCTION_SYSTEM,
             ejp.PRODUCTION_SYSTEM) ProductionSystem,
    coalesce(em.sn_id,
             ejp.j_id) JournalNumberPRS,
    coalesce(case when em.trans_fr is not null and em.trans_fr_doc_id is not null then concat("EM_", em.trans_fr,"/",cast(em.trans_fr_doc_id as string)) else null end,
             transfers2.TransferredFrom) TransferredFrom,
    coalesce(em.trans_fr_doc_id,
             transfers2.from_ms_id) TransferredFromDocumentID,
    coalesce(case when em.trans_to is not null and em.trans_to_doc_id is not null then concat("EM_", em.trans_to,"/",cast(em.trans_to_doc_id as string)) else null end,
             transfers.TransferredTo) TransferredTo,
    substr(replace(transfers.transfer_dt,'.','-'),1,10) TransferDate,
    replace(cast(em.TRANS_STAT_OUT as string), "", "0") TransferStatusOutbound,
-- NEXT SHOULD BE FINE-TUNED.
    concat("EM_", replace(substr(em.global_ms_id,4,25),"_","/")) GlobalSubmissionID,
-- NEEDS GLOBALSUBMISSIONID FROM THE TRANSFERRED FROM (SEE ABAP IN BW)
    em.FINAL_DEC_TERM FinalDecisionTerm,
    "Y" Physical,
    em.first_dec_fam FirstDecisionFamily,
    em.first_dec_term FirstDecisionTerm,
    substr(em.first_dec_dat,1,10) FirstDecisionDate,
    em. FRST_DEC_H_ED_R FirstDecisionHandlingEditorRole,
    em. HAND_ED_R CurrentHandlingEditorRole,
    em. FIN_DEC_H_ED_R FinalDecisionHandlingRole,
    cast(substr(em.trans_o_exp_d,1,10) as date) TransferOfferExpiryDate,
    case when em.trans_o_accept = 1 then "Y"
         when em.trans_o_accept = 0 then "N"
         else null
    end TransferOfferAccepted,
    case when em.trans_o_decl = 1 then "Y"
         when em.trans_o_decl = 0 then "N"
         else null
    end TransferOfferDeclined,
    case when em.trans_o_exp = 1 then "Y"
         when em.trans_o_exp = 0 then "N"
         else null
    end TransferOfferExpired,
    case when em.trans_o_resc = 1 then "Y"
         when em.trans_o_resc = 0 then "N"
         else null
    end TransferOfferRescinded,
    upper(em.trans_targ_off) TransferTargetOffered,
    case when em.MIN_ONE_REV_INV = 1 then "Y"
         when em.MIN_ONE_REV_INV = 0 then "N"
         else null
    end ReviewersInvited,
    case when em.MIN_ONE_REV_ACC = 1 then "Y"
         when em.MIN_ONE_REV_ACC = 1 then "N"
         else null
    end ReviewersAccepted,
    em.apc_id ManuscriptIDBMC,
    em.cor_au_institute Institution,
    em.comments Comment,
    case when em.status_removed = 1 then "Y"
         when em.status_removed = 0 then "N"
         else null
    end StatusRemoved,
    cast(substr(em.min_one_rev_inv_dat,1,10) as date) ReviewersInvitedDate,
    cast(substr(em.min_one_rev_acc_dat,1,10) as date) ReviewersAcceptedDate,
    cast(substr(em.min_one_rev_com_dat,1,10) as date) ReviewersCompletedDate,
    coalesce(em.lastuploaddate,
             ejp.lastuploaddate) LastUploadDate,
    substr(presubm. SUBM_QCCOMPL_DT, 1, 10) QCCompleteDate
FROM
    getsubmissionkey bk
LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.em_submissions` em
ON
    bk.SubmissionKey = upper(replace(CONCAT("EM_", em.ID), "-", "/"))
AND
    bk.LatestRevision = em.revision
LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.ejp_submissions` ejp
ON
    bk.SubmissionKey = upper(CONCAT("EJP_", upper(ejp.dbname),"/", cast(ejp.ms_id as string)))
AND
    bk.LatestRevision = ejp.ms_rev_no
LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.jfw_journals` jfw
ON
    em.flc = jfw.reference_key
LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_mapping_ejp` ejp_map
ON
    ejp.j_id = ejp_map.ejp_j_id
LEFT JOIN
    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.ejp_presubmissions` presubm
ON
    bk.SubmissionKey = upper(CONCAT("EJP_", upper(presubm.dbname),"/", cast(presubm.ms_id as string)))
AND
    bk.LatestRevision = presubm.current_ms_rev_no
LEFT JOIN
    transfers
ON
    bk.SubmissionKey = transfers.TransferredFrom
--AND
--    bk.LatestRevision = transfers.from_ms_rev_no
LEFT JOIN
    transfers2
ON
    bk.SubmissionKey = transfers2.TransferredTo
--AND
--    bk.LatestRevision = transfers2.ms_rev_no
--LEFT JOIN
--    `{{ params.project }}.{{ params.environment }}_LDZ_ProductMaster.texts_stage` stage
--ON
--    cast(ejp.current_stage_id as string) = stage.stage_id
)
SELECT
  *
FROM
  getcolumns