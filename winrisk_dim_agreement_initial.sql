/*
    FileName: party_winrisk_dim_agreement.sql
    Author: MM14295
    Subject Area : Party
    Source: WINRISK
    Create Date:2021-11-15
       
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    TERSUN-3708             Party-Tier2    15/11                Initial version     
    ------------------------------------------------------------------------------------------------------------------
*/

/* pull Winrisk (36) data from tersun */

SELECT ANALYZE_STATISTICS ('PROD_NBR_VW_TERSUN.NB_PRTY_APPL_RLE_VW');

DROP TABLE IF EXISTS TEMP_TBL_SRC;

CREATE LOCAL TEMPORARY TABLE TEMP_TBL_SRC ON COMMIT PRESERVE ROWS AS
/*+DIRECT*/
SELECT 
 CLEAN_STRING(HLDG_KEY_PFX)                                                     AS AGREEMENT_NR_PFX
,LPAD(CLEAN_STRING(HLDG_KEY),20,'0')	                                        AS AGREEMENT_NR
,CLEAN_STRING(HLDG_KEY_SFX)                                                     AS AGREEMENT_NR_SFX
,COALESCE(CLEAN_STRING(CARR_ADMIN_SYS_CD),'Unk')                                AS AGREEMENT_SOURCE_CDE
,CLEAN_STRING('APPL')	                                                        AS AGREEMENT_TYPE_CDE
,AGMT_ISS_DT::DATE                                                              AS ISSUE_DT
,AGMT_STUS_DT::TIMESTAMP(6)                                                     AS application_status_dtm
,APPL_SUBM_DT::TIMESTAMP(6)                                                     AS application_submit_dtm
,APPL_SIGND_DT::TIMESTAMP(6)                                                    AS application_signed_dtm
,APPL_RCV_DT::TIMESTAMP(6)                                                      AS application_received_dtm
,CLEAN_STRING(APPL_TYP)	                                                        AS APPLICATION_TYPE_CDE
,CLEAN_STRING(REPL_TYP)	                                                        AS REPLACEMENT_TYPE_CDE
,CLEAN_STRING('NB')                                                             AS AGREEMENT_STATUS_CDE
,NULL                                                                           AS SOURCE_AGREEMENT_STATUS_CDE
,CLEAN_STRING(AGMT_STUS_CD)                                                     AS AGREEMENT_STATUS_REASON_CDE
,CLEAN_STRING(SRC_AGMT_STUS_CD)                                                 AS SOURCE_AGREEMENT_STATUS_REASON_CDE
,CAS_ID::VARCHAR    	                                                        AS APPLICATION_CASE_ID
,CLEAN_STRING(AGMT_FRM)	                                                        AS AGREEMENT_FORM_CDE
,AGMT_INIT_DT::TIMESTAMP(6)                                                     AS application_initial_dtm
,APPL_ADE_DT::TIMESTAMP(6)                                                      AS application_data_entry_dtm
,APPL_APPV_DT::TIMESTAMP(6)                                                     AS application_approval_dtm
,APPL_DCLN_DT::TIMESTAMP(6)                                                     AS application_declined_dtm
,APPL_OFFR_DT::TIMESTAMP(6)                                                     AS application_offered_dtm
,APPL_ISS_DT::TIMESTAMP(6)                                                      AS application_issue_dtm
,APPL_INCMP_DT::TIMESTAMP(6)                                                    AS application_incomplete_dtm
,APPL_WDRW_DT::TIMESTAMP(6)                                                     AS application_withdrawal_dtm
,APPL_NOT_TAKEN_DT::TIMESTAMP(6)                                                AS application_not_taken_dtm
,APPL_RPT_DT::TIMESTAMP(6)                                                      AS application_report_dtm
,APPL_DLV_DT::TIMESTAMP(6)                                                      AS application_delivery_dtm
,APPL_FREE_LK_DT::TIMESTAMP(6)                                                  AS application_free_look_dtm
,SRC_APPL_SIGN_ST                                                               AS SOURCE_APPLICATION_SIGNED_STATE_CDE
,CLEAN_STRING(APPL_SIGN_ST)                                                     AS APPLICATION_SIGNED_STATE_CDE	
,CLEAN_STRING(E_SIGN_IND)                                                       AS ELECTRONIC_SIGNED_CDE
,PPD_IND::BOOLEAN                                                               AS PREPAID_IND
,UWTR_IND::BOOLEAN                                                              AS UNDERWRITTING_IND
,TOT_RISK_AMT::NUMERIC(17,4)	                                                AS TOTAL_RISK_AMT
,SLCT_AGY::VARCHAR(10)                                                          AS SLCT_AGY
,EZ_APP_IND::BOOLEAN	                                                        AS EASY_APPLICATION_IND
,TOP_BL_IND::BOOLEAN	                                                        AS TOP_BLUE_IND
,BRKRG_IND::BOOLEAN                                                             AS BROKERAGE_IND
,CLEAN_STRING(OWN_SAME_AS_INS_IND)::BOOLEAN                                     AS OWNER_ALIKE_INSURED_IND
,CLEAN_STRING(PAY_SAME_AS_INS_OR_OWN_IND)                                       AS PAYEE_ALIKE_INSURED_OR_OWNER_CDE 
,DSGN_LANG                                                                      AS DESIGNATION_LANGUAGE_TXT_DECRY
,CLEAN_STRING(SRC_APPL_TYP)                                                     AS SOURCE_APPLICATION_TYPE_CDE
,CLEAN_STRING(SRC_PROD_CD)                                                      AS SOURCE_PRODUCT_SHORT_NM
,CLEAN_STRING(RPT_PLCM_STUS_CD)	                                                AS REPORT_PLACEMENT_STATUS_CDE
,CLEAN_STRING(RPT_INV_STUS_CD)	                                                AS REPORT_INVENTORY_STATUS_CDE
,CLEAN_STRING(UWRT_TYP)	                                                        AS UNDERWRITING_TYPE_CDE
,CLEAN_STRING(POL_STUS_BY)                                                      AS POLICY_STATUS_BY_USER_TXT
,CLEAN_STRING(SRC_AGMT_FRM)                                                     AS SOURCE_AGREEMENT_FORM_CDE
,GRP_INFO_STRT_DT::TIMESTAMP(6)                                                 AS application_group_information_start_dtm
,CLEAN_STRING(RJCT_RSN_CD)                                                      AS REJECTED_REASON_CDE
,APPL_INIT_RVW_STRT_DT::TIMESTAMP(6)                                            AS application_initial_reviewed_start_dtm
,APPL_INIT_RVW_END_DT::TIMESTAMP(6)                                             AS application_initial_reviewed_end_dtm
,CLEAN_STRING(FLUIDLESS_IND)::BOOLEAN	                                        AS FLUIDLESS_IND
,CLEAN_STRING(SRC_ST_CD)                                                        AS SOURCE_AGREEMENT_STATE_CDE
,CLEAN_STRING(AGMT_ST)	                                                        AS AGREEMENT_STATE_CDE
,CLEAN_STRING(APPL_RISC_CLS)	                                                AS APPLICATION_RISK_CLASS_CDE
,CASE WHEN CLEAN_STRING(REPL_IND) = 'Y' THEN TRUE ELSE FALSE END                AS REPLACEMENT_IND
,CLEAN_STRING(GRP_NB)	                                                        AS APPLICATION_GROUP_NR
,CASE WHEN CLEAN_STRING(SPLT_IND) = 'Y' THEN TRUE ELSE FALSE END                AS SPLIT_CONTRACT_IND
,CLEAN_STRING(PROD_CD)	                                                        AS PRODUCT_CDE
,APPL_DATA_FR_DT::DATE	                                                        AS BEGIN_DT
,APPL_DATA_FR_DT::TIMESTAMP(6)	                                                AS BEGIN_DTM
,CURRENT_TIMESTAMP(6)	                                                        AS ROW_PROCESS_DTM
,FALSE::BOOLEAN	                                                                AS LOGICAL_DELETE_IND
,CASE WHEN APPL_DATA_TO_DT::DATE='12-31-9999' THEN TRUE ELSE FALSE END          AS CURRENT_ROW_IND
,APPL_DATA_TO_DT::DATE	                                                        AS END_DT
,APPL_DATA_TO_DT::TIMESTAMP(6)	                                                AS END_DTM
,'324'                                                                          AS SOURCE_SYSTEM_ID
,FALSE::BOOLEAN                                                                 AS RESTRICTED_ROW_IND
,CASE WHEN CLEAN_STRING(SRC_DEL_IND) = 'Y' THEN TRUE ELSE FALSE END             AS SOURCE_DELETE_IND
FROM PROD_NBR_VW_TERSUN.NB_APPL_VW SRC
WHERE SRC_SYS_ID='36'
ORDER BY CARR_ADMIN_SYS_CD,HLDG_KEY_PFX,HLDG_KEY_SFX,SOURCE_APPLICATION_SIGNED_STATE_CDE,DESIGNATION_LANGUAGE_TXT_DECRY,SLCT_AGY,HLDG_KEY;

UPDATE TEMP_TBL_SRC
SET 
SOURCE_APPLICATION_SIGNED_STATE_CDE =CLEAN_STRING(VOLTAGEACCESS(SOURCE_APPLICATION_SIGNED_STATE_CDE,'state'))
,SLCT_AGY=LPAD(VOLTAGEACCESS(SLCT_AGY,'sorparty'), 10, '0')
,DESIGNATION_LANGUAGE_TXT_DECRY=CLEAN_STRING(VOLTAGEACCESS(DESIGNATION_LANGUAGE_TXT_DECRY,'freeform'));

COMMIT;

DROP TABLE IF EXISTS MASTERS_XREF;

CREATE LOCAL TEMPORARY TABLE MASTERS_XREF ON COMMIT PRESERVE ROWS AS
/*+DIRECT*/
SELECT DISTINCT 
	VOLTAGEACCESS(SOR_PARTY_ID ,'sorparty') AS SOR_PARTY_ID 
	,DIM_PARTY_NATURAL_KEY_HASH_UUID AS SOLICITING_AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID
FROM EDW.PARTY_MASTER_OF_MASTERS_XREF 
WHERE CURRENT_ROW_IND AND LOGICAL_DELETE_IND = FALSE AND PARTY_ID_TYPE_CDE='Agency'
ORDER BY SOR_PARTY_ID,SOLICITING_AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID;

SELECT ANALYZE_STATISTICS ('TEMP_TBL_SRC');
SELECT ANALYZE_STATISTICS ('MASTERS_XREF');

DROP TABLE IF EXISTS TEMP_TBL;

CREATE LOCAL TEMPORARY TABLE TEMP_TBL ON COMMIT PRESERVE ROWS AS
SELECT  
 SRC.*,XREF.SOLICITING_AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID
,VOLTAGEPROTECT(DESIGNATION_LANGUAGE_TXT_DECRY,'freeform')  AS DESIGNATION_LANGUAGE_TXT
FROM TEMP_TBL_SRC SRC
LEFT JOIN MASTERS_XREF XREF
ON  SLCT_AGY= SOR_PARTY_ID
ORDER BY AGREEMENT_SOURCE_CDE,AGREEMENT_TYPE_CDE,AGREEMENT_NR_PFX,AGREEMENT_NR,AGREEMENT_NR_SFX,APPLICATION_CASE_ID;

SELECT ANALYZE_STATISTICS ('TEMP_TBL');

DROP TABLE IF EXISTS TEMP_TBL_SRC;

DROP TABLE IF EXISTS MASTERS_XREF;


/* uuid and checksum generation */
 
DROP TABLE IF EXISTS FULL_DIM_AGREEMENT;

CREATE LOCAL TEMPORARY TABLE FULL_DIM_AGREEMENT ON COMMIT PRESERVE ROWS AS 
/*+DIRECT*/
SELECT * FROM 
(
	SELECT DISTINCT
	 UUID_GEN(AGREEMENT_SOURCE_CDE,AGREEMENT_TYPE_CDE,AGREEMENT_NR_PFX,AGREEMENT_NR,AGREEMENT_NR_SFX,APPLICATION_CASE_ID)::UUID DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
	,AGREEMENT_NR_PFX
	,AGREEMENT_NR
	,AGREEMENT_NR_SFX
	,AGREEMENT_SOURCE_CDE
	,AGREEMENT_TYPE_CDE
	,ISSUE_DT
	,application_status_dtm
	,application_submit_dtm
	,application_signed_dtm
	,application_received_dtm
	,APPLICATION_TYPE_CDE
	,REPLACEMENT_TYPE_CDE
	,AGREEMENT_STATUS_CDE
	,SOURCE_AGREEMENT_STATUS_CDE
	,AGREEMENT_STATUS_REASON_CDE
	,SOURCE_AGREEMENT_STATUS_REASON_CDE
	,APPLICATION_CASE_ID
	,AGREEMENT_FORM_CDE
	,application_initial_dtm
	,application_data_entry_dtm
	,application_approval_dtm
	,application_declined_dtm
	,application_offered_dtm
	,application_issue_dtm
	,application_incomplete_dtm
	,application_withdrawal_dtm
	,application_not_taken_dtm
	,application_report_dtm
	,application_delivery_dtm
	,application_free_look_dtm
	,SOURCE_APPLICATION_SIGNED_STATE_CDE
	,APPLICATION_SIGNED_STATE_CDE	
	,ELECTRONIC_SIGNED_CDE
	,PREPAID_IND
	,UNDERWRITTING_IND
	,TOTAL_RISK_AMT
	,SOLICITING_AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID
	,EASY_APPLICATION_IND
	,TOP_BLUE_IND
	,BROKERAGE_IND
	,OWNER_ALIKE_INSURED_IND
	,PAYEE_ALIKE_INSURED_OR_OWNER_CDE
	,DESIGNATION_LANGUAGE_TXT
	,SOURCE_APPLICATION_TYPE_CDE
	,SOURCE_PRODUCT_SHORT_NM
	,REPORT_PLACEMENT_STATUS_CDE
	,REPORT_INVENTORY_STATUS_CDE
	,UNDERWRITING_TYPE_CDE
	,POLICY_STATUS_BY_USER_TXT
	,SOURCE_AGREEMENT_FORM_CDE
	,application_group_information_start_dtm
	,REJECTED_REASON_CDE
	,application_initial_reviewed_start_dtm
	,application_initial_reviewed_end_dtm
	,FLUIDLESS_IND
	,SOURCE_AGREEMENT_STATE_CDE
	,AGREEMENT_STATE_CDE
	,APPLICATION_RISK_CLASS_CDE
	,REPLACEMENT_IND
	,APPLICATION_GROUP_NR
	,SPLIT_CONTRACT_IND
	,PRODUCT_CDE
	,BEGIN_DT
	,BEGIN_DTM
	,ROW_PROCESS_DTM
	,LOGICAL_DELETE_IND
	,NULL::UUID AS CHECK_SUM
	,CURRENT_ROW_IND
	,END_DT
	,END_DTM
	,SOURCE_SYSTEM_ID
	,RESTRICTED_ROW_IND
	,SOURCE_DELETE_IND
	FROM TEMP_TBL
	)SUB
ORDER BY AGREEMENT_SOURCE_CDE,AGREEMENT_TYPE_CDE,AGREEMENT_NR_PFX,AGREEMENT_NR_SFX, APPLICATION_CASE_ID,AGREEMENT_NR,DIM_AGREEMENT_NATURAL_KEY_HASH_UUID;

UPDATE FULL_DIM_AGREEMENT
SET CHECK_SUM=UUID_GEN(SOURCE_DELETE_IND,ISSUE_DT,application_status_dtm,application_submit_dtm,application_signed_dtm,application_received_dtm,APPLICATION_TYPE_CDE,REPLACEMENT_TYPE_CDE
	,AGREEMENT_STATUS_CDE,SOURCE_AGREEMENT_STATUS_CDE,AGREEMENT_STATUS_REASON_CDE,SOURCE_AGREEMENT_STATUS_REASON_CDE,AGREEMENT_FORM_CDE,application_initial_dtm,application_data_entry_dtm
	,application_approval_dtm,application_declined_dtm,application_offered_dtm,application_issue_dtm,application_incomplete_dtm,application_withdrawal_dtm,application_not_taken_dtm,application_report_dtm
	,application_delivery_dtm,application_free_look_dtm,SOURCE_APPLICATION_SIGNED_STATE_CDE,APPLICATION_SIGNED_STATE_CDE,ELECTRONIC_SIGNED_CDE,PREPAID_IND,UNDERWRITTING_IND,TOTAL_RISK_AMT
	,SOLICITING_AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID::VARCHAR,EASY_APPLICATION_IND,TOP_BLUE_IND,BROKERAGE_IND,OWNER_ALIKE_INSURED_IND,PAYEE_ALIKE_INSURED_OR_OWNER_CDE,DESIGNATION_LANGUAGE_TXT
	,SOURCE_APPLICATION_TYPE_CDE,SOURCE_PRODUCT_SHORT_NM,REPORT_PLACEMENT_STATUS_CDE,REPORT_INVENTORY_STATUS_CDE,UNDERWRITING_TYPE_CDE,POLICY_STATUS_BY_USER_TXT,SOURCE_AGREEMENT_FORM_CDE
	,application_group_information_start_dtm,REJECTED_REASON_CDE,application_initial_reviewed_start_dtm,application_initial_reviewed_end_dtm,FLUIDLESS_IND,SOURCE_AGREEMENT_STATE_CDE,AGREEMENT_STATE_CDE,APPLICATION_RISK_CLASS_CDE
	,REPLACEMENT_IND,APPLICATION_GROUP_NR,SPLIT_CONTRACT_IND,PRODUCT_CDE)::UUID;

COMMIT;

DROP TABLE TEMP_TBL;

SELECT ANALYZE_STATISTICS ('FULL_DIM_AGREEMENT');

/* generatr RW_NUM to calculate the end_dt */

DROP TABLE IF EXISTS RW_DIM_AGREEMENT;

CREATE LOCAL TEMPORARY TABLE RW_DIM_AGREEMENT ON COMMIT PRESERVE ROWS AS 
/*+DIRECT*/
SELECT A.*, ROW_NUMBER() OVER(PARTITION BY DIM_AGREEMENT_NATURAL_KEY_HASH_UUID ORDER BY BEGIN_DTM, END_DTM) AS RW_NUM 
FROM FULL_DIM_AGREEMENT A 
ORDER BY AGREEMENT_SOURCE_CDE,AGREEMENT_TYPE_CDE,AGREEMENT_NR_PFX,AGREEMENT_NR_SFX, APPLICATION_CASE_ID,AGREEMENT_NR,DIM_AGREEMENT_NATURAL_KEY_HASH_UUID;

DROP TABLE FULL_DIM_AGREEMENT;

SELECT ANALYZE_STATISTICS ('RW_DIM_AGREEMENT');


/* truncate work */

TRUNCATE TABLE EDW_WORK.PARTY_WINRISK_DIM_AGREEMENT;

COMMIT;

/* insert from temp to work and calculate current_row_ind, End_dt and End_dtm as per Standards */

INSERT /*+direct*/ INTO EDW_WORK.PARTY_WINRISK_DIM_AGREEMENT
(
 DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
,AGREEMENT_NR_PFX
,AGREEMENT_NR
,AGREEMENT_NR_SFX
,AGREEMENT_SOURCE_CDE
,AGREEMENT_TYPE_CDE
,ISSUE_DT
,application_status_dtm
,application_submit_dtm
,application_signed_dtm
,application_received_dtm
,APPLICATION_TYPE_CDE
,REPLACEMENT_TYPE_CDE
,AGREEMENT_STATUS_CDE
,SOURCE_AGREEMENT_STATUS_CDE
,AGREEMENT_STATUS_REASON_CDE
,SOURCE_AGREEMENT_STATUS_REASON_CDE
,APPLICATION_CASE_ID
,AGREEMENT_FORM_CDE
,application_initial_dtm
,application_data_entry_dtm
,application_approval_dtm
,application_declined_dtm
,application_offered_dtm
,application_issue_dtm
,application_incomplete_dtm
,application_withdrawal_dtm
,application_not_taken_dtm
,application_report_dtm
,application_delivery_dtm
,application_free_look_dtm
,SOURCE_APPLICATION_SIGNED_STATE_CDE
,APPLICATION_SIGNED_STATE_CDE	
,ELECTRONIC_SIGNED_CDE
,PREPAID_IND
,UNDERWRITTING_IND
,TOTAL_RISK_AMT
,SOLICITING_AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID
,EASY_APPLICATION_IND
,TOP_BLUE_IND
,BROKERAGE_IND
,OWNER_ALIKE_INSURED_IND
,PAYEE_ALIKE_INSURED_OR_OWNER_CDE
,DESIGNATION_LANGUAGE_TXT
,SOURCE_APPLICATION_TYPE_CDE
,SOURCE_PRODUCT_SHORT_NM
,REPORT_PLACEMENT_STATUS_CDE
,REPORT_INVENTORY_STATUS_CDE
,UNDERWRITING_TYPE_CDE
,POLICY_STATUS_BY_USER_TXT
,SOURCE_AGREEMENT_FORM_CDE
,application_group_information_start_dtm
,REJECTED_REASON_CDE
,application_initial_reviewed_start_dtm
,application_initial_reviewed_end_dtm
,FLUIDLESS_IND
,SOURCE_AGREEMENT_STATE_CDE
,AGREEMENT_STATE_CDE
,APPLICATION_RISK_CLASS_CDE
,REPLACEMENT_IND
,APPLICATION_GROUP_NR
,SPLIT_CONTRACT_IND
,PRODUCT_CDE
,BEGIN_DT
,BEGIN_DTM
,ROW_PROCESS_DTM
,AUDIT_ID
,LOGICAL_DELETE_IND
,CHECK_SUM
,CURRENT_ROW_IND
,END_DT
,END_DTM
,SOURCE_SYSTEM_ID
,RESTRICTED_ROW_IND
,UPDATE_AUDIT_ID
,SOURCE_DELETE_IND
)
SELECT
 A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
,A.AGREEMENT_NR_PFX
,A.AGREEMENT_NR
,A.AGREEMENT_NR_SFX
,A.AGREEMENT_SOURCE_CDE
,A.AGREEMENT_TYPE_CDE
,A.ISSUE_DT
,A.application_status_dtm
,A.application_submit_dtm
,A.application_signed_dtm
,A.application_received_dtm
,A.APPLICATION_TYPE_CDE
,A.REPLACEMENT_TYPE_CDE
,A.AGREEMENT_STATUS_CDE
,A.SOURCE_AGREEMENT_STATUS_CDE
,A.AGREEMENT_STATUS_REASON_CDE
,A.SOURCE_AGREEMENT_STATUS_REASON_CDE
,A.APPLICATION_CASE_ID
,A.AGREEMENT_FORM_CDE
,A.application_initial_dtm
,A.application_data_entry_dtm
,A.application_approval_dtm
,A.application_declined_dtm
,A.application_offered_dtm
,A.application_issue_dtm
,A.application_incomplete_dtm
,A.application_withdrawal_dtm
,A.application_not_taken_dtm
,A.application_report_dtm
,A.application_delivery_dtm
,A.application_free_look_dtm
,A.SOURCE_APPLICATION_SIGNED_STATE_CDE
,A.APPLICATION_SIGNED_STATE_CDE	
,A.ELECTRONIC_SIGNED_CDE
,A.PREPAID_IND
,A.UNDERWRITTING_IND
,A.TOTAL_RISK_AMT
,A.SOLICITING_AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID
,A.EASY_APPLICATION_IND
,A.TOP_BLUE_IND
,A.BROKERAGE_IND
,A.OWNER_ALIKE_INSURED_IND
,A.PAYEE_ALIKE_INSURED_OR_OWNER_CDE
,A.DESIGNATION_LANGUAGE_TXT
,A.SOURCE_APPLICATION_TYPE_CDE
,A.SOURCE_PRODUCT_SHORT_NM
,A.REPORT_PLACEMENT_STATUS_CDE
,A.REPORT_INVENTORY_STATUS_CDE
,A.UNDERWRITING_TYPE_CDE
,A.POLICY_STATUS_BY_USER_TXT
,A.SOURCE_AGREEMENT_FORM_CDE
,A.application_group_information_start_dtm
,A.REJECTED_REASON_CDE
,A.application_initial_reviewed_start_dtm
,A.application_initial_reviewed_end_dtm
,A.FLUIDLESS_IND
,A.SOURCE_AGREEMENT_STATE_CDE
,A.AGREEMENT_STATE_CDE
,A.APPLICATION_RISK_CLASS_CDE
,A.REPLACEMENT_IND
,A.APPLICATION_GROUP_NR
,A.SPLIT_CONTRACT_IND
,A.PRODUCT_CDE
,A.BEGIN_DT
,A.BEGIN_DTM
,A.ROW_PROCESS_DTM
,:audit_id                                                AS AUDIT_ID
,A.LOGICAL_DELETE_IND
,A.CHECK_SUM
,CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 THEN FALSE ELSE A.CURRENT_ROW_IND END AS CURRENT_ROW_IND
,CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND  A.END_DT > B.BEGIN_DT - INTERVAL '1' DAY THEN B.BEGIN_DT - INTERVAL '1' DAY 
      ELSE A.END_DT END AS END_DT
,CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND  A.END_DTM > B.BEGIN_DTM - INTERVAL '1' SECOND THEN B.BEGIN_DTM - INTERVAL '1' SECOND 
      ELSE A.END_DTM END AS END_DTM
,A.SOURCE_SYSTEM_ID
,A.RESTRICTED_ROW_IND
,:audit_id                                                AS UPDATE_AUDIT_ID
,A.SOURCE_DELETE_IND
FROM RW_DIM_AGREEMENT A
LEFT JOIN RW_DIM_AGREEMENT B
ON A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID=B.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
AND A.RW_NUM=B.RW_NUM-1;  

COMMIT;

SELECT ANALYZE_STATISTICS ('EDW_WORK.PARTY_WINRISK_DIM_AGREEMENT');

DROP TABLE RW_DIM_AGREEMENT;

/* delete winrisk data in core table */

DELETE FROM EDW.DIM_AGREEMENT WHERE SOURCE_SYSTEM_ID in ('324','36');

COMMIT;


/* insert into core table from work */

INSERT /*+direct*/ INTO EDW.DIM_AGREEMENT
(
 DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
,AGREEMENT_NR_PFX
,AGREEMENT_NR
,AGREEMENT_NR_SFX
,AGREEMENT_SOURCE_CDE
,AGREEMENT_TYPE_CDE
,ISSUE_DT
,application_status_dtm
,application_submit_dtm
,application_signed_dtm
,application_received_dtm
,APPLICATION_TYPE_CDE
,REPLACEMENT_TYPE_CDE
,AGREEMENT_STATUS_CDE
,SOURCE_AGREEMENT_STATUS_CDE
,AGREEMENT_STATUS_REASON_CDE
,SOURCE_AGREEMENT_STATUS_REASON_CDE
,APPLICATION_CASE_ID
,AGREEMENT_FORM_CDE
,application_initial_dtm
,application_data_entry_dtm
,application_approval_dtm
,application_declined_dtm
,application_offered_dtm
,application_issue_dtm
,application_incomplete_dtm
,application_withdrawal_dtm
,application_not_taken_dtm
,application_report_dtm
,application_delivery_dtm
,application_free_look_dtm
,SOURCE_APPLICATION_SIGNED_STATE_CDE
,APPLICATION_SIGNED_STATE_CDE	
,ELECTRONIC_SIGNED_CDE
,PREPAID_IND
,UNDERWRITTING_IND
,TOTAL_RISK_AMT
,SOLICITING_AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID
,EASY_APPLICATION_IND
,TOP_BLUE_IND
,BROKERAGE_IND
,OWNER_ALIKE_INSURED_IND
,PAYEE_ALIKE_INSURED_OR_OWNER_CDE
,DESIGNATION_LANGUAGE_TXT
,SOURCE_APPLICATION_TYPE_CDE
,SOURCE_PRODUCT_SHORT_NM
,REPORT_PLACEMENT_STATUS_CDE
,REPORT_INVENTORY_STATUS_CDE
,UNDERWRITING_TYPE_CDE
,POLICY_STATUS_BY_USER_TXT
,SOURCE_AGREEMENT_FORM_CDE
,application_group_information_start_dtm
,REJECTED_REASON_CDE
,application_initial_reviewed_start_dtm
,application_initial_reviewed_end_dtm
,FLUIDLESS_IND
,SOURCE_AGREEMENT_STATE_CDE
,AGREEMENT_STATE_CDE
,APPLICATION_RISK_CLASS_CDE
,REPLACEMENT_IND
,APPLICATION_GROUP_NR
,SPLIT_CONTRACT_IND
,PRODUCT_CDE
,BEGIN_DT
,BEGIN_DTM
,ROW_PROCESS_DTM
,AUDIT_ID
,LOGICAL_DELETE_IND
,CHECK_SUM
,CURRENT_ROW_IND
,END_DT
,END_DTM
,SOURCE_SYSTEM_ID
,RESTRICTED_ROW_IND
,UPDATE_AUDIT_ID
,SOURCE_DELETE_IND
)
SELECT 
 DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
,AGREEMENT_NR_PFX
,AGREEMENT_NR
,AGREEMENT_NR_SFX
,AGREEMENT_SOURCE_CDE
,AGREEMENT_TYPE_CDE
,ISSUE_DT
,application_status_dtm
,application_submit_dtm
,application_signed_dtm
,application_received_dtm
,APPLICATION_TYPE_CDE
,REPLACEMENT_TYPE_CDE
,AGREEMENT_STATUS_CDE
,SOURCE_AGREEMENT_STATUS_CDE
,AGREEMENT_STATUS_REASON_CDE
,SOURCE_AGREEMENT_STATUS_REASON_CDE
,APPLICATION_CASE_ID
,AGREEMENT_FORM_CDE
,application_initial_dtm
,application_data_entry_dtm
,application_approval_dtm
,application_declined_dtm
,application_offered_dtm
,application_issue_dtm
,application_incomplete_dtm
,application_withdrawal_dtm
,application_not_taken_dtm
,application_report_dtm
,application_delivery_dtm
,application_free_look_dtm
,SOURCE_APPLICATION_SIGNED_STATE_CDE
,APPLICATION_SIGNED_STATE_CDE	
,ELECTRONIC_SIGNED_CDE
,PREPAID_IND
,UNDERWRITTING_IND
,TOTAL_RISK_AMT
,SOLICITING_AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID
,EASY_APPLICATION_IND
,TOP_BLUE_IND
,BROKERAGE_IND
,OWNER_ALIKE_INSURED_IND
,PAYEE_ALIKE_INSURED_OR_OWNER_CDE
,DESIGNATION_LANGUAGE_TXT
,SOURCE_APPLICATION_TYPE_CDE
,SOURCE_PRODUCT_SHORT_NM
,REPORT_PLACEMENT_STATUS_CDE
,REPORT_INVENTORY_STATUS_CDE
,UNDERWRITING_TYPE_CDE
,POLICY_STATUS_BY_USER_TXT
,SOURCE_AGREEMENT_FORM_CDE
,application_group_information_start_dtm
,REJECTED_REASON_CDE
,application_initial_reviewed_start_dtm
,application_initial_reviewed_end_dtm
,FLUIDLESS_IND
,SOURCE_AGREEMENT_STATE_CDE
,AGREEMENT_STATE_CDE
,APPLICATION_RISK_CLASS_CDE
,REPLACEMENT_IND
,APPLICATION_GROUP_NR
,SPLIT_CONTRACT_IND
,PRODUCT_CDE
,BEGIN_DT
,BEGIN_DTM
,ROW_PROCESS_DTM
,AUDIT_ID
,LOGICAL_DELETE_IND
,CHECK_SUM
,CURRENT_ROW_IND
,END_DT
,END_DTM
,SOURCE_SYSTEM_ID
,RESTRICTED_ROW_IND
,UPDATE_AUDIT_ID
,SOURCE_DELETE_IND
FROM EDW_WORK.PARTY_WINRISK_DIM_AGREEMENT;

COMMIT;