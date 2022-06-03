/*
    FileName: party_cdavntg1life_party_master_of_masters_xref.sql
    Author: MM69917
    Subject Area : Party
    Source:CDA VNTG1
    Create Date:2021-09-06
       
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    TERSUN-3525             Party-Tier2    09/06                Initial version      
    ------------------------------------------------------------------------------------------------------------------
*/

/* truncate pre_work and work */

TRUNCATE TABLE EDW_STAGING.PARTY_CDAVNTG1LIFE_PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK;
TRUNCATE TABLE EDW_WORK.PARTY_CDAVNTG1LIFE_PARTY_MASTER_OF_MASTERS_XREF;


INSERT /*+direct*/ INTO EDW_STAGING.PARTY_CDAVNTG1LIFE_PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK
(
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    PARTY_ID,
    PARTY_PRIOR_ID,
    SOR_PARTY_ID,
    PARTY_ID_TYPE_CDE,
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    CHECK_SUM,
    CURRENT_ROW_IND,
    END_DT,
    END_DTM,
    SOURCE_SYSTEM_ID,
    RESTRICTED_ROW_IND,
    UPDATE_AUDIT_ID,
    SOURCE_DELETE_IND
)
SELECT       
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    PARTY_ID,
    PARTY_PRIOR_ID,
    SOR_PARTY_ID,
    PARTY_ID_TYPE_CDE,
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    CHECK_SUM,
    CURRENT_ROW_IND,
    END_DT,
    END_DTM,
    SOURCE_SYSTEM_ID,
    RESTRICTED_ROW_IND,
    UPDATE_AUDIT_ID,
    SOURCE_DELETE_IND
FROM 
(
SELECT 
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    VOLTAGEPROTECT(PARTY_ID,'sorparty')       AS PARTY_ID,
    VOLTAGEPROTECT(PARTY_PRIOR_ID,'sorparty') AS PARTY_PRIOR_ID,
    VOLTAGEPROTECT(SOR_PARTY_ID,'sorparty')   AS SOR_PARTY_ID,
    PARTY_ID_TYPE_CDE,
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    CHECK_SUM,
    CURRENT_ROW_IND,
    END_DT,
    END_DTM,
    SOURCE_SYSTEM_ID,
    RESTRICTED_ROW_IND,
    UPDATE_AUDIT_ID,
    SOURCE_DELETE_IND,
    ROW_NUMBER() OVER(PARTITION BY DIM_PARTY_NATURAL_KEY_HASH_UUID, PARTY_ID, PARTY_ID_TYPE_CDE ORDER BY BENEFICIARY_EFFECTIVE_DT) RNK
	FROM
(
SELECT 
    CASE WHEN bene_name_first IS NULL 
         AND bene_name_middle IS NULL 
         AND bene_name_last IS NULL 
         AND bene_row_adm_sys_name IS NULL 
         AND bene_row_ctrt_prefix IS NULL 
         AND bene_row_ctrt_no IS NULL 
         AND bene_row_ctrt_suffix IS NULL 
         AND bene_arrngmt IS NULL 
         AND bene_row_cntr IS NULL 
         THEN UUID_GEN(NULL)::UUID 
         ELSE UUID_GEN(bene_name_first, bene_name_middle, bene_name_last, 
                       bene_row_adm_sys_name, bene_row_ctrt_prefix, bene_row_ctrt_no, bene_row_ctrt_suffix, 
                       bene_arrngmt, bene_row_cntr)::UUID END AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
    CASE WHEN  PARTY_ID_TYPE_CDE = 'Beneficiary' 
									OR 
		 ( bene_name_first IS NULL 
         AND bene_name_middle IS NULL 
         AND bene_name_last IS NULL 
         AND bene_row_adm_sys_name IS NULL
         AND bene_row_ctrt_prefix IS NULL 
         AND bene_row_ctrt_no IS NULL 
         AND bene_row_ctrt_suffix IS NULL 
         AND bene_arrngmt IS NULL 
         AND bene_row_cntr IS NULL )
         THEN UUID_GEN(NULL)::UUID 
         ELSE UUID_GEN(bene_name_first, bene_name_middle, bene_name_last, 
                       bene_row_adm_sys_name, bene_row_ctrt_prefix, bene_row_ctrt_no, bene_row_ctrt_suffix, 
                       bene_arrngmt, bene_row_cntr)::UUID END AS DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    COALESCE(bene_name_first,'')||COALESCE(bene_name_middle,'')||COALESCE(bene_name_last,'')||
    COALESCE(bene_row_adm_sys_name,'')||COALESCE(bene_row_ctrt_prefix,'')||COALESCE(bene_row_ctrt_no,'')||COALESCE(bene_row_ctrt_suffix,'')||
    COALESCE(SUBSTRING(bene_arrngmt,1,100),'')||COALESCE(bene_row_cntr,'')  AS PARTY_ID, 
    CASE WHEN party_id_type_cde = 'Mstr_prty_id' 
         THEN COALESCE(bene_name_first,'')||COALESCE(bene_name_middle,'')||COALESCE(bene_name_last,'')||
              COALESCE(bene_row_adm_sys_name,'')||COALESCE(bene_row_ctrt_prefix,'')||COALESCE(bene_row_ctrt_no,'')||COALESCE(bene_row_ctrt_suffix,'')||
              COALESCE(SUBSTRING(bene_arrngmt,1,100),'')||COALESCE(bene_row_cntr,'')
         ELSE NULL END             AS PARTY_PRIOR_ID, 
    NULL                           AS SOR_PARTY_ID,
    party_id_type_cde              AS PARTY_ID_TYPE_CDE,
    '0001-01-01'::DATE             AS BEGIN_DT, 
    '0001-01-01'::TIMESTAMP(6)     AS BEGIN_DTM, 
    CURRENT_TIMESTAMP              AS ROW_PROCESS_DTM, 
    :audit_id                      AS AUDIT_ID, 
    FALSE::BOOLEAN                 AS LOGICAL_DELETE_IND, 
    UUID_GEN(FALSE::BOOLEAN)::UUID AS CHECK_SUM,
    TRUE::BOOLEAN                  AS CURRENT_ROW_IND,
    '12/31/9999'::DATE             AS END_DT,
    '12/31/9999 00:00:00'::TIMESTAMP(6) AS END_DTM,
    '38'                           AS SOURCE_SYSTEM_ID,
    FALSE::BOOLEAN                 AS RESTRICTED_ROW_IND,	
    :audit_id                      AS UPDATE_AUDIT_ID, 
    FALSE::BOOLEAN                 AS SOURCE_DELETE_IND, 
    beneficiary_effective_dt       AS BENEFICIARY_EFFECTIVE_DT
FROM 
(
    SELECT 
    CLEAN_STRING(VOLTAGEACCESS(bene_name_first,'name'))            AS BENE_NAME_FIRST, 
    CLEAN_STRING(VOLTAGEACCESS(bene_name_middle,'name'))           AS BENE_NAME_MIDDLE,
    CLEAN_STRING(VOLTAGEACCESS(bene_name_last,'name'))             AS BENE_NAME_LAST, 
    LPAD(CLEAN_STRING(bene_row_ctrt_no),20,'0')                    AS BENE_ROW_CTRT_NO,
    CLEAN_STRING(VOLTAGEACCESS(bene_arrngmt,'freeform'))           AS BENE_ARRNGMT,
    CLEAN_STRING(bene_row_cntr::VARCHAR)                           AS BENE_ROW_CNTR,
    bene_eff_dt                                                    AS BENEFICIARY_EFFECTIVE_DT, 
    'Mstr_prty_id'                                                 AS PARTY_ID_TYPE_CDE,
    UDF_ISNUM_LPAD(CLEAN_STRING(bene_row_ctrt_prefix),20,'0',TRUE) AS BENE_ROW_CTRT_PREFIX,
    UDF_ISNUM_LPAD(CLEAN_STRING(bene_row_ctrt_suffix),20,'0',TRUE) AS BENE_ROW_CTRT_SUFFIX,
    CLEAN_STRING(bene_row_adm_sys_name)                            AS BENE_ROW_ADM_SYS_NAME
    FROM EDW_STAGING.cda_vntg1_life_edw_bene_delta  SRC
    
    UNION 
    
    SELECT 
    CLEAN_STRING(VOLTAGEACCESS(bene_name_first,'name'))            AS BENE_NAME_FIRST, 
    CLEAN_STRING(VOLTAGEACCESS(bene_name_middle,'name'))           AS BENE_NAME_MIDDLE,
    CLEAN_STRING(VOLTAGEACCESS(bene_name_last,'name'))             AS BENE_NAME_LAST, 
    LPAD(CLEAN_STRING(bene_row_ctrt_no),20,'0')                    AS BENE_ROW_CTRT_NO,
    CLEAN_STRING(VOLTAGEACCESS(bene_arrngmt,'freeform'))           AS BENE_ARRNGMT,
    CLEAN_STRING(bene_row_cntr::VARCHAR)                           AS BENE_ROW_CNTR,
    BENE_EFF_DT                                                    AS BENEFICIARY_EFFECTIVE_DT, 
    'Beneficiary'                                                  AS PARTY_ID_TYPE_CDE,
    UDF_ISNUM_LPAD(CLEAN_STRING(bene_row_ctrt_prefix),20,'0',TRUE) AS BENE_ROW_CTRT_PREFIX,
    UDF_ISNUM_LPAD(CLEAN_STRING(bene_row_ctrt_suffix),20,'0',TRUE) AS BENE_ROW_CTRT_SUFFIX,
    CLEAN_STRING(bene_row_adm_sys_name)                            AS BENE_ROW_ADM_SYS_NAME
    FROM EDW_STAGING.cda_vntg1_life_edw_bene_delta SRC
)SRC_DATASET
)FINAL_DATASET 
)DEDUP WHERE RNK=1;

COMMIT;

SELECT ANALYZE_STATISTICS('EDW_STAGING.PARTY_CDAVNTG1LIFE_PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK');

/* insert new records into work  */

INSERT /*+direct*/ INTO EDW_WORK.PARTY_CDAVNTG1LIFE_PARTY_MASTER_OF_MASTERS_XREF
(
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    PARTY_ID,
    PARTY_PRIOR_ID,
    SOR_PARTY_ID,
    PARTY_ID_TYPE_CDE,
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    CHECK_SUM,
    CURRENT_ROW_IND,
    END_DT,
    END_DTM,
    SOURCE_SYSTEM_ID,
    RESTRICTED_ROW_IND,
    UPDATE_AUDIT_ID,
    PARTY_ID_CLASS_TYPE_CDE,
    PARTY_ID_STATUS_TYPE_CDE,
    SOURCE_PARTY_ID_TYPE_CDE,
    SOURCE_PARTY_ID_STATUS_TYPE_CDE,
    SOURCE_PARTY_ID_CLASS_TYPE_CDE,
    SOURCE_DELETE_IND
)
SELECT 
    SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID,
    SRC.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID,
    SRC.PARTY_ID,
    SRC.PARTY_PRIOR_ID,
    SRC.SOR_PARTY_ID,
    SRC.PARTY_ID_TYPE_CDE,
    SRC.BEGIN_DT,
    SRC.BEGIN_DTM,
    SRC.ROW_PROCESS_DTM,
    SRC.AUDIT_ID,
    SRC.LOGICAL_DELETE_IND,
    SRC.CHECK_SUM,
    SRC.CURRENT_ROW_IND,
    SRC.END_DT,
    SRC.END_DTM,
    SRC.SOURCE_SYSTEM_ID,
    SRC.RESTRICTED_ROW_IND,
    SRC.UPDATE_AUDIT_ID,
    SRC.PARTY_ID_CLASS_TYPE_CDE,
    SRC.PARTY_ID_STATUS_TYPE_CDE,
    SRC.SOURCE_PARTY_ID_TYPE_CDE,
    SRC.SOURCE_PARTY_ID_STATUS_TYPE_CDE,
    SRC.SOURCE_PARTY_ID_CLASS_TYPE_CDE,
    SRC.SOURCE_DELETE_IND
FROM EDW_STAGING.PARTY_CDAVNTG1LIFE_PARTY_MASTER_OF_MASTERS_XREF_PRE_WORK SRC
LEFT JOIN EDW.PARTY_MASTER_OF_MASTERS_XREF XREF 
ON SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID = XREF.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND SRC.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID = XREF.DIM_PRIOR_PARTY_NATURAL_KEY_HASH_UUID
AND SRC.PARTY_ID = XREF.PARTY_ID
AND COALESCE(SRC.SOR_PARTY_ID,'-') = COALESCE(XREF.SOR_PARTY_ID,'-')
AND COALESCE(SRC.PARTY_ID_TYPE_CDE,'-') = COALESCE(XREF.PARTY_ID_TYPE_CDE,'-')
AND XREF.SOURCE_SYSTEM_ID IN ('38','241')
WHERE XREF.ROW_SID IS NULL; 

COMMIT;
         
SELECT ANALYZE_STATISTICS('EDW_WORK.PARTY_CDAVNTG1LIFE_PARTY_MASTER_OF_MASTERS_XREF');