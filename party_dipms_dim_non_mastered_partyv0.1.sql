/*
    FileName: party_dipms_dim_non_mastered_party.sql
    Author: mma2156
    SUBJECT AREA : Party
    Table_Name :dim_non_mastered_party
    SOURCE: dipms
    Teradata Source Code: 77                       
    Create Date:2022-03-07
        
    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    		               Party-Tier2         29/03           First Version Tier-2 
    ------------------------------------------------------------------------------------------------------------------
*/ 

SELECT ANALYZE_STATISTICS('PROD_NBR_VW_TERSUN.NB_APPL_PRTY_VW');

DROP TABLE IF EXISTS PRE_WORK1;

CREATE LOCAL TEMPORARY TABLE PRE_WORK1 ON COMMIT PRESERVE ROWS AS
SELECT * FROM EDW_WORK.PARTY_DIPMS_DIM_NON_MASTERED_PARTY WHERE 1<>1;



INSERT /*DIRECT*/ INTO PRE_WORK1(
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
SOURCE_MARITAL_STATUS_CDE,
GOVERNMENT_ID,
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
PARTY_TYPE_CDE
)
SELECT
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
SOURCE_MARITAL_STATUS_CDE,
GOVERNMENT_ID,
BEGIN_DT,
BEGIN_DTM,
ROW_PROCESS_DTM,
AUDIT_ID,
LOGICAL_DELETE_IND,
UUID_GEN(SOURCE_DELETE_IND,FIRST_NM,MIDDLE_NM,LAST_NM,FULL_NM,BIRTH_DT,GENDER_CDE,SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,MARITAL_STATUS_CDE,SOURCE_MARITAL_STATUS_CDE,GOVERNMENT_ID,SOURCE_LOGIN_ID,PARTY_TYPE_CDE)::UUID 
AS CHECK_SUM, 
CURRENT_ROW_IND,
END_DT,
END_DTM,
SOURCE_SYSTEM_ID,
RESTRICTED_ROW_IND,
UPDATE_AUDIT_ID,
SOURCE_DELETE_IND,
PARTY_TYPE_CDE                                               -- rownumber to remove row level duplicates
FROM(
SELECT 
UUID_GEN(DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID)::UUID AS DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID, --UUID GENERATED ON DECRYPTED VALUES
PARTY_ID,
FIRST_NM,
NULL AS MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
NULL AS SOURCE_LOGIN_ID,
SOURCE_MARITAL_STATUS_CDE,
GOVERNMENT_ID,
BEGIN_DT,
BEGIN_DTM,
CURRENT_TIMESTAMP(6) AS ROW_PROCESS_DTM,
-1 AS AUDIT_ID,                                          -- default this to -1
FALSE AS LOGICAL_DELETE_IND,
CASE WHEN END_DT = '9999-12-31' THEN TRUE ELSE FALSE END AS CURRENT_ROW_IND,                                        -- logic as per mapping doc
END_DT,
END_DTM,
'342' AS SOURCE_SYSTEM_ID,
FALSE AS RESTRICTED_ROW_IND,
-1 AS UPDATE_AUDIT_ID,                                   -- default this to -1
SOURCE_DELETE_IND,
'I' AS PARTY_TYPE_CDE,
ROW_NUMBER() OVER (PARTITION BY DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,PARTY_ID,FIRST_NM, LAST_NM, FULL_NM,
 BIRTH_DT, GENDER_CDE,SENSITIVE_PARTY_IND ,SOURCE_GENDER_CDE, MARITAL_STATUS_CDE,SOURCE_MARITAL_STATUS_CDE,
 GOVERNMENT_ID,SOURCE_DELETE_IND ORDER BY BEGIN_DTM,END_DTM DESC) AS RNK
FROM (
SELECT 
CLEAN_STRING(VOLTAGEACCESS(SYS_PRTY_ID,'sorparty')) AS DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(SYS_PRTY_ID,'sorparty')),'sorparty') AS PARTY_ID,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(FRST_NM,'name')),'name')  AS FIRST_NM,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(LST_NM,'name')),'name') AS LAST_NM,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(ENTY_NM,'name')),'name') AS FULL_NM,
VOLTAGEPROTECTDATE(VOLTAGEACCESSDATE(DOB,'dob_date'),'dob_date') AS BIRTH_DT,                                               -- voltage protect?
CLEAN_STRING(GNDR_CD) AS GENDER_CDE,
(CASE WHEN SNS='Y' THEN TRUE ELSE FALSE END) AS SENSITIVE_PARTY_IND ,
CLEAN_STRING(SRC_GNDR) AS SOURCE_GENDER_CDE,
CLEAN_STRING(MARI_STUS_CD) AS MARITAL_STATUS_CDE,
CLEAN_STRING(SRC_MARI_STUS_CD) AS SOURCE_MARITAL_STATUS_CDE,
VOLTAGEPROTECT(CLEAN_STRING(VOLTAGEACCESS(GOVT_ID,'ssn_char')),'ssn_char') AS GOVERNMENT_ID,
PRTY_DATA_FR_DT ::DATE AS BEGIN_DT,
PRTY_DATA_FR_DT ::TIMESTAMP(6) AS BEGIN_DTM,
PRTY_DATA_TO_DT ::DATE END_DT,
PRTY_DATA_TO_DT ::TIMESTAMP(6) AS END_DTM,
(CASE WHEN CLEAN_STRING(SRC_DEL_IND)='Y' THEN TRUE ELSE FALSE END) AS SOURCE_DELETE_IND
FROM PROD_NBR_VW_TERSUN.NB_APPL_PRTY_VW WHERE SRC_SYS_ID = 77
)Q_1)Q_2
WHERE RNK=1;




SELECT ANALYZE_STATISTICS('PROD_NBR_VW_TERSUN.NB_PRTY_BENE_VW');



INSERT /*DIRECT*/ INTO PRE_WORK1(
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
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
PARTY_TYPE_CDE
)
SELECT 
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
BEGIN_DT,
BEGIN_DTM,
ROW_PROCESS_DTM,
AUDIT_ID,
LOGICAL_DELETE_IND,
UUID_GEN(SOURCE_DELETE_IND,FIRST_NM,MIDDLE_NM,LAST_NM,FULL_NM,BIRTH_DT,GENDER_CDE,SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,MARITAL_STATUS_CDE,SOURCE_MARITAL_STATUS_CDE,GOVERNMENT_ID,SOURCE_LOGIN_ID,PARTY_TYPE_CDE)::UUID 
AS CHECK_SUM,
CURRENT_ROW_IND, 
END_DT,
END_DTM,
SOURCE_SYSTEM_ID,
RESTRICTED_ROW_IND,
UPDATE_AUDIT_ID,
SOURCE_DELETE_IND,
PARTY_TYPE_CDE                                            -- add rownumber to remove row level duplicates
FROM (
SELECT 
UUID_GEN(PARTY_ID)::UUID AS DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,  --UUID GENERATED ON DECRYPTED VALUES
VOLTAGEPROTECT(PARTY_ID,'sorparty') AS PARTY_ID,                                                                          ----- encryption
VOLTAGEPROTECT(FIRST_NM ,'name') AS FIRST_NM,
NULL AS MIDDLE_NM,
VOLTAGEPROTECT(LAST_NM,'name') AS LAST_NM,
NULL AS FULL_NM,
NULL AS BIRTH_DT,
NULL AS GENDER_CDE,
NULL AS SENSITIVE_PARTY_IND,
NULL AS SOURCE_GENDER_CDE,
NULL AS SOURCE_MARITAL_STATUS_CDE,
NULL AS SOURCE_LOGIN_ID,
NULL AS MARITAL_STATUS_CDE,
NULL AS GOVERNMENT_ID,
BEGIN_DT,
BEGIN_DTM,
CURRENT_TIMESTAMP(6)  AS ROW_PROCESS_DTM,
-1 AS AUDIT_ID,                                    -- default it to -1
FALSE AS LOGICAL_DELETE_IND,
CASE WHEN END_DT  = '9999-12-31' THEN TRUE ELSE FALSE END AS CURRENT_ROW_IND,                                  -- change the logic according to mapping doc
END_DT,
END_DTM,
'342' AS SOURCE_SYSTEM_ID,
FALSE AS RESTRICTED_ROW_IND,
-1 AS UPDATE_AUDIT_ID,                             -- default it to -1  
FALSE AS SOURCE_DELETE_IND,
'I' AS PARTY_TYPE_CDE,
ROW_NUMBER() OVER (PARTITION BY FIRST_NM,LAST_NM,PARTY_ID ORDER BY BEGIN_DTM,END_DTM) AS RNK
FROM(
SELECT 
CLEAN_STRING(VOLTAGEACCESS(SRC.FRST_NM,'name'))AS FIRST_NM,
CLEAN_STRING(VOLTAGEACCESS(SRC.LST_NM,'name')) AS LAST_NM,
SRC.BENE_FR_DT::DATE AS BEGIN_DT,
SRC.BENE_FR_DT::TIMESTAMP(6) AS BEGIN_DTM,
SRC.BENE_TO_DT::DATE AS END_DT,
SRC.BENE_TO_DT::TIMESTAMP(6) AS END_DTM,
CLEAN_STRING(APPL.HLDG_KEY) || SRC.BENE_ID || COALESCE (FIRST_NM,'')|| COALESCE (LAST_NM,'')  AS PARTY_ID         -- decryption
FROM 
PROD_NBR_VW_TERSUN.NB_PRTY_BENE_VW SRC 
LEFT JOIN 
(SELECT DISTINCT HLDG_KEY,APPL_ID FROM PROD_NBR_VW_TERSUN.NB_APPL_VW)APPL 
ON SRC.APPL_ID = APPL.APPL_ID 
WHERE SRC_SYS_ID = 77)Q_1
)Q_2
WHERE RNK=1;





SELECT ANALYZE_STATISTICS('PROD_NBR_VW_TERSUN.NB_PRTY_CASE_OWN_VW');



INSERT /*DIRECT*/ INTO PRE_WORK1(
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
SOURCE_LOGIN_ID,
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
PARTY_TYPE_CDE
)
SELECT 
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
SOURCE_LOGIN_ID,
BEGIN_DT,
BEGIN_DTM,
ROW_PROCESS_DTM,
AUDIT_ID,
LOGICAL_DELETE_IND,
UUID_GEN(SOURCE_DELETE_IND,FIRST_NM,MIDDLE_NM,LAST_NM,FULL_NM,BIRTH_DT,GENDER_CDE,SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,MARITAL_STATUS_CDE,SOURCE_MARITAL_STATUS_CDE,GOVERNMENT_ID,SOURCE_LOGIN_ID,PARTY_TYPE_CDE)::UUID
AS CHECK_SUM,
CURRENT_ROW_IND,
END_DT,
END_DTM,
SOURCE_SYSTEM_ID,
RESTRICTED_ROW_IND,
UPDATE_AUDIT_ID,
SOURCE_DELETE_IND,
PARTY_TYPE_CDE                                                                -- add rownumber and remove duplicates
FROM(
SELECT
UUID_GEN(PARTY_ID)::UUID AS DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,  --UUID GENERATED ON DECRYPTED VALUES
VOLTAGEPROTECT(PARTY_ID,'sorparty') AS PARTY_ID,
VOLTAGEPROTECT(FIRST_NM,'name') AS FIRST_NM,
NULL AS MIDDLE_NM,
VOLTAGEPROTECT(LAST_NM,'name') AS LAST_NM,
NULL AS FULL_NM,
NULL AS BIRTH_DT,
NULL AS GENDER_CDE,
NULL AS SENSITIVE_PARTY_IND,
NULL AS SOURCE_GENDER_CDE,
NULL AS MARITAL_STATUS_CDE,
NULL AS GOVERNMENT_ID,
NULL AS SOURCE_MARITAL_STATUS_CDE,
VOLTAGEPROTECT(SOURCE_LOGIN_ID,'account_char') AS SOURCE_LOGIN_ID,
'0001-01-01'::DATE AS BEGIN_DT,
'0001-01-01'::TIMESTAMP(6) AS BEGIN_DTM,
CURRENT_TIMESTAMP(6) AS  ROW_PROCESS_DTM,
-1 AS AUDIT_ID,
FALSE AS LOGICAL_DELETE_IND,
TRUE AS CURRENT_ROW_IND,
'999-12-31'::DATE AS END_DT,
'999-12-31'::TIMESTAMP(6) AS END_DTM,
'342' AS SOURCE_SYSTEM_ID,
FALSE AS RESTRICTED_ROW_IND,
-1 AS UPDATE_AUDIT_ID,
FALSE AS SOURCE_DELETE_IND,
'I' AS PARTY_TYPE_CDE 
FROM (
SELECT DISTINCT
CLEAN_STRING(VOLTAGEACCESS(FRST_NM,'name')) AS FIRST_NM,
CLEAN_STRING(VOLTAGEACCESS(LST_NM,'name')) AS LAST_NM,
CLEAN_STRING(VOLTAGEACCESS(LOG_IN_ID,'account_char')) AS SOURCE_LOGIN_ID,
COALESCE(FIRST_NM,'') || COALESCE(LAST_NM,'') || COALESCE(SOURCE_LOGIN_ID,'') AS PARTY_ID                          -- decrypted values
FROM prod_nbr_vw_tersun.NB_PRTY_CASE_OWN_VW WHERE SRC_SYS_ID = 77
)Q_1
)Q_2;


INSERT /*DIRECT*/ INTO PRE_WORK1(
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
SOURCE_LOGIN_ID,
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
PARTY_TYPE_CDE,
GROUP_NBR,
SUB_GROUP_NBR,
GROUP_IPN_ID,
GROUP_TYPE_CDE,
GROUP_TYPE_EFFECTIVE_DT,
DISCOUNT_PCT,
BILL_AT_ISSUE_IND,
PLAN_TYPE_CDE,
MAXIMUM_BENEFIT_PAID_IND,
BUY_SELL_GROUP_TYPE_CDE,
LONG_TERM_CARE_IND,
DEFAULT_DIVIDEND_PCT,
GUARANTEED_STANDARD_ISSUE_IND,
EMPLOYER_PAID_PCT,
EMPLOYER_PAID_DISCOUNT_PCT,
PREEXISTING_CONDITION_LIMITATION_ID,
DUE_DT_ALIGNMENT_IND,
SERVICING_AGENCY_ID,
SOURCE_MARKET_CDE,
MARKET_CDE,
MARKET_CDE_EFFECTIVE_DT,
SOURCE_SALES_CATEGORY_CDE,
SALES_CATEGORY_CDE,
SALES_CATEGORY_EFFECTIVE_DT,
DEFAULT_DIVIDEND_PCT_EFFECTIVE_DT,
UNDERWRITING_PROCESSOR_ID,
PARENT_GROUP_NR_ID,
ERISA_PLAN_CDE,
ERISA_PLAN_EFFECTIVE_DT,
EMPLOYER_PAID_EFFECTIVE_DT,
EMPLOYER_PAID_DISCOUNT_EFFECTIVE_DT,
SALARY_DEDUCTION_IND,
MGI_IND,
ENDR_IND,
ENDR_DT,
SERVICING_AGENT_ID,
SOURCE_BILLING_FREQUENCY_CDE,
BILLING_FREQUENCY_CDE,
SOURCE_BILL_TYPE_CDE,
BILLING_TYPE_CDE,
LEVEL_PCT,
CROSSOVER_YEAR_TXT,
EMPLOYEE_RECEIVE_DIVIDEND_IND,
EMPLOYEE_RECEIVE_PREMIUM_IND,
SIC_CDE,
GROUP_CLASS_CDE,
BEGIN_BILLING_DT,
SECOND_DUE_DAY_TXT
)
SELECT
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
VOLTAGEPROTECT(PARTY_ID,'sorparty') AS PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
SOURCE_LOGIN_ID,
BEGIN_DT,
BEGIN_DTM,
ROW_PROCESS_DTM,
AUDIT_ID,
LOGICAL_DELETE_IND,
UUID_GEN(SOURCE_DELETE_IND,FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
SOURCE_LOGIN_ID,
PARTY_TYPE_CDE,
GROUP_NBR,
SUB_GROUP_NBR,
GROUP_IPN_ID,
GROUP_TYPE_CDE,
GROUP_TYPE_EFFECTIVE_DT,
DISCOUNT_PCT,
BILL_AT_ISSUE_IND,
PLAN_TYPE_CDE,
MAXIMUM_BENEFIT_PAID_IND,
BUY_SELL_GROUP_TYPE_CDE,
LONG_TERM_CARE_IND,
DEFAULT_DIVIDEND_PCT,
GUARANTEED_STANDARD_ISSUE_IND,
EMPLOYER_PAID_PCT,
EMPLOYER_PAID_DISCOUNT_PCT,
PREEXISTING_CONDITION_LIMITATION_ID,
DUE_DT_ALIGNMENT_IND,
SERVICING_AGENCY_ID,
SOURCE_MARKET_CDE,
MARKET_CDE,
MARKET_CDE_EFFECTIVE_DT,
SOURCE_SALES_CATEGORY_CDE,
SALES_CATEGORY_CDE,
SALES_CATEGORY_EFFECTIVE_DT,
DEFAULT_DIVIDEND_PCT_EFFECTIVE_DT,
UNDERWRITING_PROCESSOR_ID,
PARENT_GROUP_NR_ID,
ERISA_PLAN_CDE,
ERISA_PLAN_EFFECTIVE_DT,
EMPLOYER_PAID_EFFECTIVE_DT,
EMPLOYER_PAID_DISCOUNT_EFFECTIVE_DT,
SALARY_DEDUCTION_IND,
MGI_IND,
ENDR_IND,
ENDR_DT,
SERVICING_AGENT_ID,
SOURCE_BILLING_FREQUENCY_CDE,
BILLING_FREQUENCY_CDE,
SOURCE_BILL_TYPE_CDE,
BILLING_TYPE_CDE,
LEVEL_PCT,
CROSSOVER_YEAR_TXT,
EMPLOYEE_RECEIVE_DIVIDEND_IND,
EMPLOYEE_RECEIVE_PREMIUM_IND,
SIC_CDE,
GROUP_CLASS_CDE,
BEGIN_BILLING_DT,
SECOND_DUE_DAY_TXT
)::UUID AS CHECK_SUM,
CURRENT_ROW_IND,
END_DT,
END_DTM,
SOURCE_SYSTEM_ID,
RESTRICTED_ROW_IND,
UPDATE_AUDIT_ID,
SOURCE_DELETE_IND,
PARTY_TYPE_CDE,
GROUP_NBR,
SUB_GROUP_NBR,
GROUP_IPN_ID,
GROUP_TYPE_CDE,
GROUP_TYPE_EFFECTIVE_DT,
DISCOUNT_PCT,
BILL_AT_ISSUE_IND,
PLAN_TYPE_CDE,
MAXIMUM_BENEFIT_PAID_IND,
BUY_SELL_GROUP_TYPE_CDE,
LONG_TERM_CARE_IND,
DEFAULT_DIVIDEND_PCT,
GUARANTEED_STANDARD_ISSUE_IND,
EMPLOYER_PAID_PCT,
EMPLOYER_PAID_DISCOUNT_PCT,
PREEXISTING_CONDITION_LIMITATION_ID,
DUE_DT_ALIGNMENT_IND,
SERVICING_AGENCY_ID,
SOURCE_MARKET_CDE,
MARKET_CDE,
MARKET_CDE_EFFECTIVE_DT,
SOURCE_SALES_CATEGORY_CDE,
SALES_CATEGORY_CDE,
SALES_CATEGORY_EFFECTIVE_DT,
DEFAULT_DIVIDEND_PCT_EFFECTIVE_DT,
UNDERWRITING_PROCESSOR_ID,
PARENT_GROUP_NR_ID,
ERISA_PLAN_CDE,
ERISA_PLAN_EFFECTIVE_DT,
EMPLOYER_PAID_EFFECTIVE_DT,
EMPLOYER_PAID_DISCOUNT_EFFECTIVE_DT,
SALARY_DEDUCTION_IND,
MGI_IND,
ENDR_IND,
ENDR_DT,
SERVICING_AGENT_ID,
SOURCE_BILLING_FREQUENCY_CDE,
BILLING_FREQUENCY_CDE,
SOURCE_BILL_TYPE_CDE,
BILLING_TYPE_CDE,
LEVEL_PCT,
CROSSOVER_YEAR_TXT,
EMPLOYEE_RECEIVE_DIVIDEND_IND,
EMPLOYEE_RECEIVE_PREMIUM_IND,
SIC_CDE,
GROUP_CLASS_CDE,
BEGIN_BILLING_DT,
SECOND_DUE_DAY_TXT
FROM(
SELECT 
UUID_GEN(PARTY_ID)::UUID AS DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
NULL AS FIRST_NM,
NULL AS MIDDLE_NM,
NULL AS LAST_NM,
FULL_NM,
NULL AS BIRTH_DT,
NULL AS GENDER_CDE,
NULL AS SENSITIVE_PARTY_IND,
NULL AS SOURCE_GENDER_CDE,
NULL AS MARITAL_STATUS_CDE,
NULL AS SOURCE_LOGIN_ID,
BEGIN_DT,
BEGIN_DTM,
CURRENT_TIMESTAMP(6) AS ROW_PROCESS_DTM,
-1 AS AUDIT_ID,
FALSE AS LOGICAL_DELETE_IND,
TRUE AS CURRENT_ROW_IND,
END_DT,
END_DTM,
SOURCE_SYSTEM_ID,
FALSE AS RESTRICTED_ROW_IND,
-1 AS UPDATE_AUDIT_ID,
FALSE AS SOURCE_DELETE_IND,
PARTY_TYPE_CDE,
GROUP_NBR,
SUB_GROUP_NBR,
GROUP_IPN_ID,
GROUP_TYPE_CDE,
GROUP_TYPE_EFFECTIVE_DT,
DISCOUNT_PCT,
BILL_AT_ISSUE_IND,
PLAN_TYPE_CDE,
MAXIMUM_BENEFIT_PAID_IND,
BUY_SELL_GROUP_TYPE_CDE,
LONG_TERM_CARE_IND,
DEFAULT_DIVIDEND_PCT,
GUARANTEED_STANDARD_ISSUE_IND,
EMPLOYER_PAID_PCT,
EMPLOYER_PAID_DISCOUNT_PCT,
PREEXISTING_CONDITION_LIMITATION_ID,
DUE_DT_ALIGNMENT_IND,
SERVICING_AGENCY_ID,
SOURCE_MARKET_CDE,
MARKET_CDE,
MARKET_CDE_EFFECTIVE_DT,
SOURCE_SALES_CATEGORY_CDE,
SALES_CATEGORY_CDE,
SALES_CATEGORY_EFFECTIVE_DT,
DEFAULT_DIVIDEND_PCT_EFFECTIVE_DT,
UNDERWRITING_PROCESSOR_ID,
PARENT_GROUP_NR_ID,
ERISA_PLAN_CDE,
ERISA_PLAN_EFFECTIVE_DT,
EMPLOYER_PAID_EFFECTIVE_DT,
EMPLOYER_PAID_DISCOUNT_EFFECTIVE_DT,
SALARY_DEDUCTION_IND,
MGI_IND,
ENDR_IND,
ENDR_DT,
SERVICING_AGENT_ID,
SOURCE_BILLING_FREQUENCY_CDE,
BILLING_FREQUENCY_CDE,
SOURCE_BILL_TYPE_CDE,
BILLING_TYPE_CDE,
LEVEL_PCT,
CROSSOVER_YEAR_TXT,
EMPLOYEE_RECEIVE_DIVIDEND_IND,
EMPLOYEE_RECEIVE_PREMIUM_IND,
SIC_CDE,
GROUP_CLASS_CDE,
BEGIN_BILLING_DT,
SECOND_DUE_DAY_TXT,
ROW_NUMBER() OVER(PARTITION BY PARTY_ID,FULL_NM,GROUP_NBR,SUB_GROUP_NBR,GROUP_IPN_ID,
GROUP_TYPE_CDE,GROUP_TYPE_EFFECTIVE_DT,DISCOUNT_PCT,BILL_AT_ISSUE_IND,PLAN_TYPE_CDE,
 MAXIMUM_BENEFIT_PAID_IND,BUY_SELL_GROUP_TYPE_CDE,LONG_TERM_CARE_IND,
DEFAULT_DIVIDEND_PCT,
GUARANTEED_STANDARD_ISSUE_IND,
EMPLOYER_PAID_PCT,
EMPLOYER_PAID_DISCOUNT_PCT,
PREEXISTING_CONDITION_LIMITATION_ID,
DUE_DT_ALIGNMENT_IND,
SERVICING_AGENCY_ID,
SOURCE_MARKET_CDE,
MARKET_CDE,
MARKET_CDE_EFFECTIVE_DT,
SOURCE_SALES_CATEGORY_CDE,
SALES_CATEGORY_CDE,
SALES_CATEGORY_EFFECTIVE_DT,
DEFAULT_DIVIDEND_PCT_EFFECTIVE_DT,
UNDERWRITING_PROCESSOR_ID,
PARENT_GROUP_NR_ID,
ERISA_PLAN_CDE,
ERISA_PLAN_EFFECTIVE_DT,
EMPLOYER_PAID_EFFECTIVE_DT,
EMPLOYER_PAID_DISCOUNT_EFFECTIVE_DT,
SALARY_DEDUCTION_IND,
MGI_IND,
ENDR_IND,
ENDR_DT,
SERVICING_AGENT_ID,
SOURCE_BILLING_FREQUENCY_CDE,
BILLING_FREQUENCY_CDE,
SOURCE_BILL_TYPE_CDE,
BILLING_TYPE_CDE,
LEVEL_PCT,
CROSSOVER_YEAR_TXT,
EMPLOYEE_RECEIVE_DIVIDEND_IND,
EMPLOYEE_RECEIVE_PREMIUM_IND,
SIC_CDE,
GROUP_CLASS_CDE,
BEGIN_BILLING_DT,
SECOND_DUE_DAY_TXT
ORDER BY BEGIN_DTM,END_DTM) AS RNK
FROM(
SELECT 
'Dipms_'||CLEAN_STRING(GRP_NB) AS PARTY_ID,
CLEAN_STRING(GRP_NM) AS FULL_NM,
GRP_INFO_STRT_DT::DATE AS BEGIN_DT,
GRP_INFO_STRT_DT::TIMESTAMP(6)  AS BEGIN_DTM,
GRP_INFO_END_DT::DATE AS END_DT,
GRP_INFO_END_DT::TIMESTAMP(6) AS END_DTM,
'342' AS SOURCE_SYSTEM_ID,
'N' AS PARTY_TYPE_CDE,
CLEAN_STRING(GRP_NB) AS GROUP_NBR,
CLEAN_STRING(SUB_GRP_NB) AS SUB_GROUP_NBR,
CLEAN_STRING(GRP_IPN) AS GROUP_IPN_ID,
CLEAN_STRING(GRP_TYP) AS GROUP_TYPE_CDE,
GRP_EFF_DT::DATE AS GROUP_TYPE_EFFECTIVE_DT,
CLEAN_STRING(DCNT_PCT)::NUMERIC(9,6) AS DISCOUNT_PCT, 
CASE WHEN BILL_AT_ISS = 'Y' THEN TRUE ELSE FALSE END AS BILL_AT_ISSUE_IND,
CLEAN_STRING(TYP_OF_PLN) AS PLAN_TYPE_CDE,
CASE WHEN MAX_BEN_PD_END = 'Y' THEN TRUE WHEN MAX_BEN_PD_END = 'N' THEN FALSE END AS MAXIMUM_BENEFIT_PAID_IND,
CLEAN_STRING(BSELL_GRP_TYP) AS BUY_SELL_GROUP_TYPE_CDE,
CASE WHEN  LNG_TRM_CARE_IND = 'Y' THEN TRUE WHEN LNG_TRM_CARE_IND = 'N' THEN FALSE END AS LONG_TERM_CARE_IND,
CLEAN_STRING(DFLT_DIVD_PCT)::NUMERIC(9,6) AS DEFAULT_DIVIDEND_PCT,
CASE WHEN GSI_IND = 'Y' THEN TRUE WHEN GSI_IND = 'N' THEN FALSE  END AS GUARANTEED_STANDARD_ISSUE_IND,
CLEAN_STRING(EER_PD_PCT)::NUMERIC(9,6) AS EMPLOYER_PAID_PCT,
CLEAN_STRING(EER_PD_DCNT_PCT)::NUMERIC(9,6) AS EMPLOYER_PAID_DISCOUNT_PCT,
CLEAN_STRING(PECL_IDENT) AS PREEXISTING_CONDITION_LIMITATION_ID,
CASE WHEN DUE_DT_ALGN_IND = 'Y' THEN TRUE WHEN DUE_DT_ALGN_IND = 'N' THEN FALSE  END AS DUE_DT_ALIGNMENT_IND,
CLEAN_STRING(SVCNG_AGY) AS SERVICING_AGENCY_ID,
CLEAN_STRING(SRC_MKT_CD) AS SOURCE_MARKET_CDE,
CLEAN_STRING(MKT_CD) AS MARKET_CDE,
MKT_CD_EFF_DT::DATE AS MARKET_CDE_EFFECTIVE_DT,
CLEAN_STRING(SRC_SALES_CTG_CD) AS SOURCE_SALES_CATEGORY_CDE,
CLEAN_STRING(SALES_CTG_CD) AS SALES_CATEGORY_CDE,
SALES_CTG_EFF_DT::DATE AS SALES_CATEGORY_EFFECTIVE_DT,
DFLT_DIV_PCT_EFF_DT::DATE AS DEFAULT_DIVIDEND_PCT_EFFECTIVE_DT,
CLEAN_STRING(UND_PRCR_ID) AS UNDERWRITING_PROCESSOR_ID,
CLEAN_STRING(PARNT_GRP_NR_IDENT) AS PARENT_GROUP_NR_ID,
CLEAN_STRING(ERISA_PLN_CD) AS ERISA_PLAN_CDE,
ERISA_PLN_EFF_DT::DATE AS ERISA_PLAN_EFFECTIVE_DT,
EER_PD_EFF_DT::DATE  AS EMPLOYER_PAID_EFFECTIVE_DT,
EER_PD_DCNT_EFF_DT::DATE AS EMPLOYER_PAID_DISCOUNT_EFFECTIVE_DT,
CASE WHEN SAL_DED_IND = 'Y' THEN TRUE WHEN SAL_DED_IND = 'N' THEN FALSE  END AS SALARY_DEDUCTION_IND,
CASE WHEN MGI_IND = 'Y' THEN TRUE WHEN MGI_IND = 'N' THEN FALSE  END AS MGI_IND,
CASE WHEN ENDR_IND = 'Y' THEN TRUE WHEN ENDR_IND = 'N' THEN FALSE  END AS ENDR_IND,
ENDR_DT::DATE AS ENDR_DT,
CLEAN_STRING(SVC_AGT) AS SERVICING_AGENT_ID,
CLEAN_STRING(SRC_BILL_FREQ) AS SOURCE_BILLING_FREQUENCY_CDE,
CLEAN_STRING(BILL_FREQ_CD) AS BILLING_FREQUENCY_CDE,
CLEAN_STRING(SRC_BILL_TYP) AS SOURCE_BILL_TYPE_CDE,
CLEAN_STRING(BILL_TYP_CD) AS BILLING_TYPE_CDE,
LVL_PCT::NUMERIC(9,6) AS LEVEL_PCT,
CLEAN_STRING(XOVR_YR) AS CROSSOVER_YEAR_TXT,
CASE WHEN EMP_RCV_DIV = 'Y' THEN TRUE WHEN EMP_RCV_DIV = 'N' THEN FALSE END AS EMPLOYEE_RECEIVE_DIVIDEND_IND,
CASE WHEN EMP_RCV_PREM_REF = 'Y' THEN TRUE WHEN EMP_RCV_PREM_REF = 'N' THEN FALSE  END AS EMPLOYEE_RECEIVE_PREMIUM_IND,
CLEAN_STRING(SIC_CD) AS SIC_CDE,
CLEAN_STRING(GRP_CLS) AS GROUP_CLASS_CDE,
BEG_BILL_DT::DATE AS BEGIN_BILLING_DT,
CLEAN_STRING(SECOND_DU_DY) AS SECOND_DUE_DAY_TXT 
FROM 
PROD_NBR_VW_TERSUN.NB_GRP_INFO_VW_0329)Q_1
)Q_2
WHERE RNK=1;



DROP TABLE IF EXISTS PRE_WORK2;

CREATE LOCAL TEMPORARY TABLE PRE_WORK2 ON COMMIT PRESERVE ROWS AS 
SELECT *,ROW_NUMBER() OVER(PARTITION BY DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID ORDER BY 
BEGIN_DTM,END_DTM) AS RW_NUM
FROM PRE_WORK1 ORDER BY DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID;


DROP  TABLE IF EXISTS PRE_WORK;

CREATE LOCAL TEMPORARY TABLE PRE_WORK ON COMMIT PRESERVE ROWS AS 
SELECT * FROM  EDW_WORK.PARTY_DIPMS_DIM_NON_MASTERED_PARTY WHERE 1<>1;

--

INSERT /*DIRECT*/ INTO PRE_WORK(
DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
PARTY_ID,
FIRST_NM,
MIDDLE_NM,
LAST_NM,
FULL_NM,
BIRTH_DT,
GENDER_CDE,
SENSITIVE_PARTY_IND,
SOURCE_GENDER_CDE,
MARITAL_STATUS_CDE,
SOURCE_LOGIN_ID,
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
PARTY_TYPE_CDE,
GROUP_NBR,
SUB_GROUP_NBR,
GROUP_IPN_ID,
GROUP_TYPE_CDE,
GROUP_TYPE_EFFECTIVE_DT,
DISCOUNT_PCT,
BILL_AT_ISSUE_IND,
PLAN_TYPE_CDE,
MAXIMUM_BENEFIT_PAID_IND,
BUY_SELL_GROUP_TYPE_CDE,
LONG_TERM_CARE_IND,
DEFAULT_DIVIDEND_PCT,
GUARANTEED_STANDARD_ISSUE_IND,
EMPLOYER_PAID_PCT,
EMPLOYER_PAID_DISCOUNT_PCT,
PREEXISTING_CONDITION_LIMITATION_ID,
DUE_DT_ALIGNMENT_IND,
SERVICING_AGENCY_ID,
SOURCE_MARKET_CDE,
MARKET_CDE,
MARKET_CDE_EFFECTIVE_DT,
SOURCE_SALES_CATEGORY_CDE,
SALES_CATEGORY_CDE,
SALES_CATEGORY_EFFECTIVE_DT,
DEFAULT_DIVIDEND_PCT_EFFECTIVE_DT,
UNDERWRITING_PROCESSOR_ID,
PARENT_GROUP_NR_ID,
ERISA_PLAN_CDE,
ERISA_PLAN_EFFECTIVE_DT,
EMPLOYER_PAID_EFFECTIVE_DT,
EMPLOYER_PAID_DISCOUNT_EFFECTIVE_DT,
SALARY_DEDUCTION_IND,
MGI_IND,
ENDR_IND,
ENDR_DT,
SERVICING_AGENT_ID,
SOURCE_BILLING_FREQUENCY_CDE,
BILLING_FREQUENCY_CDE,
SOURCE_BILL_TYPE_CDE,
BILLING_TYPE_CDE,
LEVEL_PCT,
CROSSOVER_YEAR_TXT,
EMPLOYEE_RECEIVE_DIVIDEND_IND,
EMPLOYEE_RECEIVE_PREMIUM_IND,
SIC_CDE,
GROUP_CLASS_CDE,
BEGIN_BILLING_DT,
SECOND_DUE_DAY_TXT
)
SELECT 
A.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID,
A.PARTY_ID,
A.FIRST_NM,
A.MIDDLE_NM,
A.LAST_NM,
A.FULL_NM,
A.BIRTH_DT,
A.GENDER_CDE,
A.SENSITIVE_PARTY_IND,
A.SOURCE_GENDER_CDE,
A.MARITAL_STATUS_CDE,
A.SOURCE_LOGIN_ID,
A.BEGIN_DT,
A.BEGIN_DTM,
A.ROW_PROCESS_DTM,
A.AUDIT_ID,
A.LOGICAL_DELETE_IND,
A.CHECK_SUM,
CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1  THEN FALSE ELSE A.CURRENT_ROW_IND END AS CURRENT_ROW_IND,
CASE WHEN A.RW_NUM<B.RW_NUM-1 AND A.RW_NUM=B.RW_NUM-1 AND A.END_DT>B.BEGIN_DT- INTERVAL  '1' DAY 
THEN B.BEGIN_DT-INTERVAL '1' DAY ELSE A.END_DT END AS  END_DT,
CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND A.END_DTM>B.END_DTM-INTERVAL '1' SECOND 
THEN B.BEGIN_DTM- INTERVAL '1' SECOND ELSE A.END_DTM END AS END_DTM,
A.SOURCE_SYSTEM_ID,
A.RESTRICTED_ROW_IND,
A.UPDATE_AUDIT_ID,
A.SOURCE_DELETE_IND,
A.PARTY_TYPE_CDE,
A.GROUP_NBR,
A.SUB_GROUP_NBR,
A.GROUP_IPN_ID,
A.GROUP_TYPE_CDE,
A.GROUP_TYPE_EFFECTIVE_DT,
A.DISCOUNT_PCT,
A.BILL_AT_ISSUE_IND,
A.PLAN_TYPE_CDE,
A.MAXIMUM_BENEFIT_PAID_IND,
A.BUY_SELL_GROUP_TYPE_CDE,
A.LONG_TERM_CARE_IND,
A.DEFAULT_DIVIDEND_PCT,
A.GUARANTEED_STANDARD_ISSUE_IND,
A.EMPLOYER_PAID_PCT,
A.EMPLOYER_PAID_DISCOUNT_PCT,
A.PREEXISTING_CONDITION_LIMITATION_ID,
A.DUE_DT_ALIGNMENT_IND,
A.SERVICING_AGENCY_ID,
A.SOURCE_MARKET_CDE,
A.MARKET_CDE,
A.MARKET_CDE_EFFECTIVE_DT,
A.SOURCE_SALES_CATEGORY_CDE,
A.SALES_CATEGORY_CDE,
A.SALES_CATEGORY_EFFECTIVE_DT,
A.DEFAULT_DIVIDEND_PCT_EFFECTIVE_DT,
A.UNDERWRITING_PROCESSOR_ID,
A.PARENT_GROUP_NR_ID,
A.ERISA_PLAN_CDE,
A.ERISA_PLAN_EFFECTIVE_DT,
A.EMPLOYER_PAID_EFFECTIVE_DT,
A.EMPLOYER_PAID_DISCOUNT_EFFECTIVE_DT,
A.SALARY_DEDUCTION_IND,
A.MGI_IND,
A.ENDR_IND,
A.ENDR_DT,
A.SERVICING_AGENT_ID,
A.SOURCE_BILLING_FREQUENCY_CDE,
A.BILLING_FREQUENCY_CDE,
A.SOURCE_BILL_TYPE_CDE,
A.BILLING_TYPE_CDE,
A.LEVEL_PCT,
A.CROSSOVER_YEAR_TXT,
A.EMPLOYEE_RECEIVE_DIVIDEND_IND,
A.EMPLOYEE_RECEIVE_PREMIUM_IND,
A.SIC_CDE,
A.GROUP_CLASS_CDE,
A.BEGIN_BILLING_DT,
A.SECOND_DUE_DAY_TXT
FROM PRE_WORK2 A
LEFT JOIN 
PRE_WORK2 B
ON A.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID=B.DIM_NON_MASTERED_PARTY_NATURAL_KEY_HASH_UUID
AND A.RW_NUM=B.RW_NUM-1;
--

