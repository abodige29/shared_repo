/*
    FileName: party_cdavntg1life_dim_party.sql
    Author: MM15263
    Subject Area : Party
    Source: CDA VNTG1 LIFE
    Create Date:2021-09-02
       
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    TERSUN-3522             Party-Tier2    09/02                Initial version      
    ------------------------------------------------------------------------------------------------------------------
*/


SELECT ANALYZE_STATISTICS('PROD_STND_VW_TERSUN.BEN_DATA_VW');

CREATE LOCAL TEMPORARY TABLE SOURCE_DATASET ON COMMIT PRESERVE ROWS AS 
/*+DIRECT*/
SELECT 
    CLEAN_STRING(VOLTAGEACCESS(ben_frst_nm,'name'))     AS BEN_FRST_NM,
    CLEAN_STRING(VOLTAGEACCESS(ben_mdl_nm,'name'))      AS BEN_MDL_NM, 
    CLEAN_STRING(VOLTAGEACCESS(ben_lst_nm,'name'))      AS BEN_LST_NM,
    CLEAN_STRING(carr_admin_sys_cd) AS CARR_ADMIN_SYS_CD, 
    UDF_ISNUM_LPAD(CLEAN_STRING(hldg_key_pfx),20,'0',TRUE) AS HLDG_KEY_PFX, 
    LPAD(CLEAN_STRING(hldg_key),20,'0')                    AS HLDG_KEY, 
    UDF_ISNUM_LPAD(CLEAN_STRING(hldg_key_sfx),20,'0',TRUE) AS HLDG_KEY_SFX, 
    CLEAN_STRING(VOLTAGEACCESS(ben_argmt_txt,'freeform'))           AS BEN_ARGMT_TXT,
    CLEAN_STRING(ben_row_cntr_cd)         AS BEN_ROW_CNTR_CD,
    COALESCE(CLEAN_STRING(gndr_cd),'Unk') AS GNDR_CD,
    COALESCE(ben_data_fr_dt,'0001-01-01')::TIMESTAMP(6)                AS BEN_DATA_FR_DT,
    COALESCE(ben_data_to_dt,'9999-12-31')::TIMESTAMP(6)                AS BEN_DATA_TO_DT,
    CLEAN_STRING(curr_ind)        AS CURR_IND,
    CLEAN_STRING(VOLTAGEACCESS(ben_pfx_nm,'name'))      AS BEN_PFX_NM,
    CLEAN_STRING(VOLTAGEACCESS(ben_sfx_nm,'name'))      AS BEN_SFX_NM,
    FALSE::BOOLEAN AS SRC_DEL_IND
FROM PROD_STND_VW_TERSUN.BEN_DATA_VW
WHERE SRC_SYS_ID='38';

CREATE LOCAL TEMPORARY TABLE DIM_PARTY_VNTG1 ON COMMIT PRESERVE ROWS AS 
/*+DIRECT*/
SELECT 
    DIM_PARTY_NATURAL_KEY_HASH_UUID,
    VOLTAGEPROTECT(PARTY_ID,'sorparty') AS PARTY_ID,
    FIRST_NM,
    MIDDLE_NM,
    LAST_NM,
    GENDER_CDE,
    BEGIN_DT,
    BEGIN_DTM,
    ROW_PROCESS_DTM,
    AUDIT_ID,
    LOGICAL_DELETE_IND,
    UUID_GEN(SOURCE_DELETE_IND, GENDER_CDE, PREFIX_NM, SUFFIX_NM)::UUID AS CHECK_SUM,
    CURRENT_ROW_IND,
    END_DT,
    END_DTM,
    SOURCE_SYSTEM_ID,
    RESTRICTED_ROW_IND,
    PREFIX_NM,
    SUFFIX_NM,
    SOURCE_DELETE_IND
FROM 
(
SELECT 
    UUID_GEN(ben_frst_nm, ben_mdl_nm, ben_lst_nm, 
	         carr_admin_sys_cd, hldg_key_pfx, hldg_key, hldg_key_sfx, 
             ben_argmt_txt, ben_row_cntr_cd)::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID, 
    COALESCE(ben_frst_nm,'')||COALESCE(ben_mdl_nm,'')||COALESCE(ben_lst_nm,'')||
    COALESCE(carr_admin_sys_cd,'')||COALESCE(hldg_key_pfx,'')||hldg_key||COALESCE(hldg_key_sfx,'')||
    COALESCE(SUBSTRING(ben_argmt_txt,1,100),'')||COALESCE(ben_row_cntr_cd,'') AS PARTY_ID,
    VOLTAGEPROTECT(ben_frst_nm,'name')           AS FIRST_NM,
    VOLTAGEPROTECT(ben_mdl_nm,'name')            AS MIDDLE_NM,
    VOLTAGEPROTECT(ben_lst_nm,'name')            AS LAST_NM,
    gndr_cd               AS GENDER_CDE,
    ben_data_fr_dt::DATE          AS BEGIN_DT,
    ben_data_fr_dt::TIMESTAMP(6)  AS BEGIN_DTM,
    CURRENT_TIMESTAMP(6)  AS ROW_PROCESS_DTM,
    :audit_id             AS AUDIT_ID,
    FALSE                 AS LOGICAL_DELETE_IND,
    TRUE::BOOLEAN         AS CURRENT_ROW_IND,
    CASE WHEN curr_ind = 'Y' AND src_del_ind = FALSE THEN '9999-12-31'::DATE
         WHEN curr_ind = 'Y' AND src_del_ind = TRUE AND ben_data_to_dt = '9999-12-31'::DATE THEN CURRENT_DATE
         WHEN ben_data_to_dt IS NULL THEN '9999-12-31'::DATE 
         ELSE ben_data_to_dt::DATE END AS END_DT, 
    CASE WHEN curr_ind = 'Y' AND src_del_ind = FALSE THEN '9999-12-31'::TIMESTAMP 
         WHEN curr_ind = 'Y' AND src_del_ind = TRUE AND ben_data_to_dt = '9999-12-31'::TIMESTAMP(6) THEN CURRENT_TIMESTAMP(6)
         WHEN ben_data_to_dt IS NULL THEN '9999-12-31'::TIMESTAMP
         ELSE ben_data_to_dt END END_DTM, 
    '241'                 AS SOURCE_SYSTEM_ID,
    FALSE                 AS RESTRICTED_ROW_IND,
    VOLTAGEPROTECT(ben_pfx_nm,'name')            AS PREFIX_NM,
    VOLTAGEPROTECT(ben_sfx_nm,'name')            AS SUFFIX_NM,
    src_del_ind           AS SOURCE_DELETE_IND,
    ROW_NUMBER() OVER(PARTITION BY ben_frst_nm, ben_mdl_nm, ben_lst_nm, carr_admin_sys_cd, hldg_key_pfx, hldg_key, hldg_key_sfx, 
                                   ben_argmt_txt, ben_row_cntr_cd, ben_pfx_nm, ben_sfx_nm 
                      ORDER BY ben_data_fr_dt, ben_data_to_dt) AS RNK						   
FROM SOURCE_DATASET
)DEDUP WHERE RNK=1;
	
	
COMMIT;

CREATE LOCAL TEMPORARY TABLE DIM_PARTY_VNTG1_FINAL_DATASET ON COMMIT PRESERVE ROWS AS 
SELECT A.*, ROW_NUMBER() OVER(PARTITION BY DIM_PARTY_NATURAL_KEY_HASH_UUID ORDER BY BEGIN_DT, END_DT) RW_NUM 
FROM DIM_PARTY_VNTG1 A
ORDER BY DIM_PARTY_NATURAL_KEY_HASH_UUID;


TRUNCATE TABLE EDW_WORK.PARTY_CDAVNTG1LIFE_DIM_PARTY;

INSERT INTO  EDW_WORK.PARTY_CDAVNTG1LIFE_DIM_PARTY
(
 DIM_PARTY_NATURAL_KEY_HASH_UUID
,PARTY_ID
,FIRST_NM
,MIDDLE_NM
,LAST_NM
,GENDER_CDE
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
,PREFIX_NM
,SUFFIX_NM
,SOURCE_DELETE_IND
)
SELECT 
 A.DIM_PARTY_NATURAL_KEY_HASH_UUID
,A.PARTY_ID
,A.FIRST_NM
,A.MIDDLE_NM
,A.LAST_NM
,A.GENDER_CDE
,A.BEGIN_DT
,A.BEGIN_DTM
,A.ROW_PROCESS_DTM
,:audit_id                                                AS AUDIT_ID
,A.LOGICAL_DELETE_IND
,A.CHECK_SUM
,CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 THEN FALSE ELSE A.CURRENT_ROW_IND END AS CURRENT_ROW_IND
,CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND  A.END_DT > B.BEGIN_DT - INTERVAL '1' DAY THEN B.BEGIN_DT - INTERVAL '1' DAY 
      WHEN B.RW_NUM IS NULL THEN '9999-12-31'::DATE 
      ELSE A.END_DT END AS END_DT
,CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND  A.END_DTM > B.BEGIN_DTM - INTERVAL '1' SECOND THEN B.BEGIN_DTM - INTERVAL '1' SECOND 
      WHEN B.RW_NUM IS NULL THEN '9999-12-31'::TIMESTAMP(6) 
      ELSE A.END_DTM END AS END_DTM
,A.SOURCE_SYSTEM_ID
,A.RESTRICTED_ROW_IND
,:audit_id                                                AS UPDATE_AUDIT_ID
,A.PREFIX_NM
,A.SUFFIX_NM
,A.SOURCE_DELETE_IND
FROM DIM_PARTY_VNTG1_FINAL_DATASET A
LEFT JOIN DIM_PARTY_VNTG1_FINAL_DATASET B
ON A.DIM_PARTY_NATURAL_KEY_HASH_UUID=B.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND A.RW_NUM=B.RW_NUM-1;

COMMIT;

DELETE FROM EDW.DIM_PARTY WHERE SOURCE_SYSTEM_ID IN ('241','38');

COMMIT;

INSERT INTO  EDW.DIM_PARTY
(
 DIM_PARTY_NATURAL_KEY_HASH_UUID
,PARTY_ID
,FIRST_NM
,MIDDLE_NM
,LAST_NM
,GENDER_CDE
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
,PREFIX_NM
,SUFFIX_NM
,SOURCE_DELETE_IND
)
SELECT 
 DIM_PARTY_NATURAL_KEY_HASH_UUID
,PARTY_ID
,FIRST_NM
,MIDDLE_NM
,LAST_NM
,GENDER_CDE
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
,PREFIX_NM
,SUFFIX_NM
,SOURCE_DELETE_IND
FROM EDW_WORK.PARTY_CDAVNTG1LIFE_DIM_PARTY;

COMMIT;