/*
    FileName: cda_lvrgvl_edw_bene_delta_snapshot.sql
    Author: MM14803
    Subject Area : Party
    Source: CDA lVRGVL LIFE
    Create Date:2021-11-23
       
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    TERSUN-3820             Party-Tier2    06/06               Initial version      
    ------------------------------------------------------------------------------------------------------------------
*/


TRUNCATE TABLE EDW_STAGING.CDA_lVRGVL_EDW_BENE_DELTA_SNAPSHOT;

create local temporary table temp on commit preserve rows as 
select * from eDW_STAGING.CDA_lVRGVL_EDW_BENE_DELTA_SNAPSHOT where 1>1;

INSERT INTO temp--EDW_STAGING.CDA_LVRGVL_EDW_BENE_DELTA_SNAPSHOT
(
 PROCESS_IND
,BENE_ROW_NAME
,BENE_ROW_CTRT_PREFIX
,BENE_ROW_CTRT_NO
,BENE_ROW_CTRT_SUFFIX
,BENE_ROW_ADM_SYS_NAME
,BENE_ROLE_CD
,BENE_ROLE_POSITION_CD
,BENE_ROLE_SEQ_NO
,BENE_SEX
,BENE_DATE_OF_BIRTH
,BENE_SSN_TIN_CD
,BENE_SSN_TIN
,BENE_NAME_FORMAT_CD
,BENE_NAME_PREFIX
,BENE_NAME_FIRST
,BENE_NAME_MIDDLE
,BENE_NAME_LAST
,BENE_NAME_SUFFIX
,BENE_PROF_DESIG
,BENE_ARRNGMT
,BENE_RELATIONSHIP
,BENE_PCT
,BENE_EFF_DT
,BENE_TRST_DT
,BENE_CLASS_CD
,BENE_SPEC_DSGNTN
,BENE_ROW_CNTR
,ROW_PROCESS_DTM
,AUDIT_ID
,SOURCE_SYSTEM_ID
,CURRENT_BATCH  
)
SELECT 
 PROCESS_IND
,BENE_ROW_NAME
,BENE_ROW_CTRT_PREFIX
,BENE_ROW_CTRT_NO
,BENE_ROW_CTRT_SUFFIX
,BENE_ROW_ADM_SYS_NAME
,BENE_ROLE_CD
,BENE_ROLE_POSITION_CD
,BENE_ROLE_SEQ_NO
,BENE_SEX
,BENE_DATE_OF_BIRTH
,BENE_SSN_TIN_CD
,BENE_SSN_TIN
,BENE_NAME_FORMAT_CD
,BENE_NAME_PREFIX
,BENE_NAME_FIRST
,BENE_NAME_MIDDLE
,BENE_NAME_LAST
,BENE_NAME_SUFFIX
,BENE_PROF_DESIG
,BENE_ARRNGMT
,BENE_RELATIONSHIP
,BENE_PCT
,BENE_EFF_DT
,BENE_TRST_DT
,BENE_CLASS_CD
,BENE_SPEC_DSGNTN
,BENE_ROW_CNTR
,ROW_PROCESS_DTM
,AUDIT_ID
,SOURCE_SYSTEM_ID
,CURRENT_BATCH 
FROM 
(
SELECT 
CLEAN_STRING(SRC_DEL_IND) AS PROCESS_IND, 
NULL                                                       AS BENE_ROW_NAME,
CLEAN_STRING(CARR_ADMIN_SYS_CD)                            AS BENE_ROW_ADM_SYS_NAME, 
CLEAN_STRING(HLDG_KEY_PFX)                                 AS BENE_ROW_CTRT_PREFIX,
CLEAN_STRING(HLDG_KEY)                                     AS BENE_ROW_CTRT_NO, 
CLEAN_STRING(HLDG_KEY_SFX)                                 AS BENE_ROW_CTRT_SUFFIX,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(BEN_PFX_NM,'name')),'name')           AS BENE_NAME_PREFIX,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(BEN_FRST_NM,'name')),'name')           AS BENE_NAME_FIRST, 
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(BEN_MDL_NM,'name')),'name')            AS BENE_NAME_MIDDLE,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(BEN_LST_NM,'name')),'name')            AS BENE_NAME_LAST,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(BEN_SFX_NM,'name')),'name')            AS BENE_NAME_SUFFIX,
CLEAN_STRING(GNDR_CD)                                      AS BENE_SEX,
NULL                                                       AS BENE_DATE_OF_BIRTH,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(BEN_ARGMT_TXT,'freeform')),'freeform')     AS BENE_ARRNGMT, 
CLEAN_STRING(BEN_ROW_CNTR_CD)::INT                         AS BENE_ROW_CNTR,
CLEAN_STRING(RLE_CD)                                       AS BENE_ROLE_CD, 
CLEAN_STRING(RLE_STYP_CD)                                  AS BENE_CLASS_CD, 
CLEAN_STRING(BEN_REL_TXT)                                  AS BENE_RELATIONSHIP, 
BEN_PCT                                                    AS BENE_PCT,
CLEAN_STRING(RLE_POS_CD)::INT                              AS BENE_ROLE_POSITION_CD, 
CLEAN_STRING(RLE_SEQ_NBR)::INT                             AS BENE_ROLE_SEQ_NO, 
BEN_EFF_DT                                                 AS BENE_EFF_DT, 
BEN_TRST_DT                                                AS BENE_TRST_DT, 
CLEAN_STRING(BEN_SPEC_DSGNTN_CD)                           AS BENE_SPEC_DSGNTN,
CLEAN_STRING(PROF_DSGN_CD)                                 AS BENE_PROF_DESIG,
NULL                                                       AS BENE_SSN_TIN_CD,
NULL                                                       AS BENE_SSN_TIN,
CLEAN_STRING(NM_FRMT_CD)                                   AS BENE_NAME_FORMAT_CD,
CURRENT_TIMESTAMP                                          AS ROW_PROCESS_DTM, 
'-1'::INT                                                  AS AUDIT_ID,
'246'                                                      AS SOURCE_SYSTEM_ID,
TRUE                                                       AS CURRENT_BATCH,
ROW_NUMBER() OVER (PARTITION BY CARR_ADMIN_SYS_CD,HLDG_KEY_PFX, HLDG_KEY, HLDG_KEY_SFX,BEN_ROW_CNTR_CD ORDER BY BEN_DATA_FR_DT DESC, BEN_DATA_TO_DT DESC) RNK
FROM PROD_STND_VW_TERSUN.BEN_DATA_VW 
WHERE SRC_SYS_ID='85'
)SRC WHERE RNK=1;

COMMIT;