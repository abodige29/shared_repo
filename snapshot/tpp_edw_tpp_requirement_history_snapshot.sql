SELECT ANALYZE_STATISTICS ('PROD_NBR_VW_TERSUN.NB_REQ_VW');
SELECT ANALYZE_STATISTICS ('PROD_NBR_VW_TERSUN.NB_APPL_VW');

TRUNCATE TABLE edw_staging.tpp_edw_tpp_requirement_history_snapshot;

commit;

drop table if exists VT_SRC;
CREATE LOCAL TEMPORARY TABLE VT_SRC ON COMMIT PRESERVE ROWS AS 
/*+direct*/
SELECT distinct
AGT.HLDG_KEY ,
AGT.CAS_ID::int,
SRC.SRC_REQ_ID,
SRC.CAS_REQ_ID,
src.src_req_stus_cd,
src.req_stus_dt,
SRC.REQ_FR_DT ,
src.req_to_dt
FROM PROD_NBR_VW_TERSUN.NB_REQ_VW SRC 
inner JOIN PROD_NBR_VW_TERSUN.NB_APPL_VW AGT
ON SRC.APPL_ID = AGT.APPL_ID
WHERE SRC.SRC_SYS_ID = '32';


INSERT /*+direct*/
INTO EDW_STAGING.tpp_edw_tpp_requirement_history_snapshot
(
    POLICY_NUMBER,
    CASE_ID,
    AGREEMENT_NB_SOURCE,
    REQUIREMENT_ID,
    CASE_REQT_ID,
    requirement_status_code,
    requirement_status_date,
    sort_num,
    LST_UPDT_DT,
    _EDAP_LOAD_TIME,
    AUDIT_ID,
    SOURCE_SYSTEM_ID,
    CURRENT_BATCH
)
SELECT 
    POLICY_NUMBER,
    CASE_ID,
    AGREEMENT_NB_SOURCE,
    REQUIREMENT_ID,
    CASE_REQT_ID,
    requirement_status_code,
    requirement_status_date,
    sort_num,
    LST_UPDT_DT,
    _EDAP_LOAD_TIME,
    AUDIT_ID,
    SOURCE_SYSTEM_ID,
    CURRENT_BATCH
FROM (
SELECT 
    HLDG_KEY          AS POLICY_NUMBER,
    CAS_ID   AS CASE_ID,
    'TPP'             AS AGREEMENT_NB_SOURCE,
    SRC_REQ_ID::int        AS REQUIREMENT_ID,
    CAS_REQ_ID::int        AS CASE_REQT_ID,
    src_req_stus_cd     			AS requirement_status_code,
    req_stus_dt::varchar(19)           AS requirement_status_date,
    REQ_FR_DT::varchar(19) AS LST_UPDT_DT,
    CURRENT_TIMESTAMP(6)  AS _EDAP_LOAD_TIME, 
    -1                    AS AUDIT_ID, 
    '347'                 AS SOURCE_SYSTEM_ID, 
    TRUE                  AS CURRENT_BATCH,
    ROW_NUMBER() OVER(PARTITION BY HLDG_KEY, CAS_ID, CAS_REQ_ID ORDER BY REQ_FR_DT , REQ_TO_DT ) AS sort_num    
FROM VT_SRC
) SOURCE_DATASET
;


COMMIT; 

SELECT ANALYZE_STATISTICS ('edw_staging.tpp_edw_tpp_requirement_history_snapshot');

commit;