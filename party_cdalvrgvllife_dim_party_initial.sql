/*
    FileName: PARTY_CDALVRGVLLIFE_DIM_PARTY.sql
    Author: MM14295
    Subject Area : Party
    Source: CDA LVRGVL LIFE
    Create Date:2021-09-06
       
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    TERSUN-3522             Party-Tier2    06/03                Initial version      
    ------------------------------------------------------------------------------------------------------------------
*/


SELECT ANALYZE_STATISTICS('PROD_STND_VW_TERSUN.BEN_DATA_VW');

CREATE LOCAL TEMPORARY TABLE SOURCE_DATASET ON COMMIT PRESERVE ROWS AS 
/*+DIRECT*/
SELECT 
CLEAN_STRING(VOLTAGEACCESS(BEN_FRST_NM,'name'))        AS BEN_FRST_NM,
CLEAN_STRING(VOLTAGEACCESS(BEN_MDL_NM,'name'))         AS BEN_MDL_NM, 
CLEAN_STRING(VOLTAGEACCESS(BEN_LST_NM,'name'))         AS BEN_LST_NM,
CLEAN_STRING(CARR_ADMIN_SYS_CD)                        AS CARR_ADMIN_SYS_CD, 
UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_PFX),20,'0',TRUE) AS HLDG_KEY_PFX, 
LPAD(CLEAN_STRING(HLDG_KEY),20,'0')                    AS HLDG_KEY, 
UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_SFX),20,'0',TRUE) AS HLDG_KEY_SFX, 
CLEAN_STRING(VOLTAGEACCESS(BEN_ARGMT_TXT,'freeform'))  AS BEN_ARGMT_TXT,
CLEAN_STRING(BEN_ROW_CNTR_CD)                          AS BEN_ROW_CNTR_CD,
COALESCE(CLEAN_STRING(gndr_cd),'Unk')                  AS GNDR_CD,
    COALESCE(ben_data_fr_dt,'0001-01-01')::TIMESTAMP(6)                AS BEN_DATA_FR_DT,
    COALESCE(ben_data_to_dt,'9999-12-31')::TIMESTAMP(6)                AS BEN_DATA_TO_DT,
CLEAN_STRING(CURR_IND)                                 AS CURR_IND,
CLEAN_STRING(VOLTAGEACCESS(BEN_PFX_NM,'name'))         AS BEN_PFX_NM,
CLEAN_STRING(VOLTAGEACCESS(BEN_SFX_NM,'name'))         AS BEN_SFX_NM,
FALSE::BOOLEAN                                         AS SRC_DEL_IND
FROM PROD_STND_VW_TERSUN.BEN_DATA_VW
WHERE SRC_SYS_ID='85';
	

DROP TABLE IF EXISTS DIM_PARTY_LVRGVL;

CREATE LOCAL TEMPORARY TABLE DIM_PARTY_LVRGVL ON COMMIT PRESERVE ROWS AS 
/*+DIRECT*/
SELECT 
UUID_GEN(PARTY_ID)::uuid AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
VOLTAGEPROTECT(PARTY_ID,'sorparty') AS PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
GENDER_CDE,
BEGIN_DT,
BEGIN_DTM,
ROW_PROCESS_DTM,
LOGICAL_DELETE_IND,
UUID_GEN(SOURCE_DELETE_IND,FIRST_NM,MIDDLE_NM,LAST_NM,GENDER_CDE,PREFIX_NM,SUFFIX_NM)::UUID AS CHECK_SUM,
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
    COALESCE(CARR_ADMIN_SYS_CD,'')||COALESCE(HLDG_KEY_PFX,'')||HLDG_KEY||COALESCE(HLDG_KEY_SFX,'')||COALESCE(BEN_ROW_CNTR_CD,'') AS PARTY_ID,
    VOLTAGEPROTECT(BEN_FRST_NM,'name')                         AS FIRST_NM,
    VOLTAGEPROTECT(BEN_MDL_NM,'name')                          AS MIDDLE_NM,
    VOLTAGEPROTECT(BEN_LST_NM,'name')                          AS LAST_NM,
    GNDR_CD                                                    AS GENDER_CDE,
    BEN_DATA_FR_DT::DATE                                       AS BEGIN_DT,
    BEN_DATA_FR_DT::TIMESTAMP(6)                               AS BEGIN_DTM,
    CURRENT_TIMESTAMP(6)                                       AS ROW_PROCESS_DTM,
    FALSE                                                      AS LOGICAL_DELETE_IND,
    TRUE::BOOLEAN                                              AS CURRENT_ROW_IND,
    ben_data_to_dt::DATE                         AS END_DT, 
    ben_data_to_dt::TIMESTAMP(6)                 AS END_DTM, 
    '246'                                                      AS SOURCE_SYSTEM_ID,
    FALSE                                                      AS RESTRICTED_ROW_IND,
    VOLTAGEPROTECT(BEN_PFX_NM,'name')                          AS PREFIX_NM,
    VOLTAGEPROTECT(BEN_SFX_NM,'name')                          AS SUFFIX_NM,
    SRC_DEL_IND                                                AS SOURCE_DELETE_IND,
    ROW_NUMBER() OVER(PARTITION BY 
                                   CARR_ADMIN_SYS_CD,HLDG_KEY_PFX,HLDG_KEY,HLDG_KEY_SFX,
                                   BEN_ROW_CNTR_CD 
                      ORDER BY BEN_DATA_FR_DT DESC,BEN_DATA_TO_DT DESC) AS RNK						   
	FROM SOURCE_DATASET
)DEDUP WHERE RNK=1;
	
COMMIT;

CREATE LOCAL TEMPORARY TABLE DIM_PARTY_LVRGVL_FINAL_DATASET ON COMMIT PRESERVE ROWS AS 
SELECT A.*, ROW_NUMBER() OVER(PARTITION BY DIM_PARTY_NATURAL_KEY_HASH_UUID ORDER BY BEGIN_DT, END_DT) RW_NUM 
FROM DIM_PARTY_LVRGVL A
ORDER BY DIM_PARTY_NATURAL_KEY_HASH_UUID;

TRUNCATE TABLE EDW_WORK.PARTY_CDALVRGVLLIFE_DIM_PARTY;


INSERT INTO  EDW_WORK.PARTY_CDALVRGVLLIFE_DIM_PARTY
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
FROM DIM_PARTY_LVRGVL_FINAL_DATASET A
LEFT JOIN DIM_PARTY_LVRGVL_FINAL_DATASET B
ON A.DIM_PARTY_NATURAL_KEY_HASH_UUID=B.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND A.RW_NUM=B.RW_NUM-1;

COMMIT;

DELETE FROM EDW.DIM_PARTY WHERE SOURCE_SYSTEM_ID IN ('246','85');

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
FROM EDW_WORK.PARTY_CDALVRGVLLIFE_DIM_PARTY;

COMMIT;