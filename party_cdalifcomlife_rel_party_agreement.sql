
/*

    FileName: party_cdalifcomlife_rel_party_agreement.sql
    Author: MM14295
    SUBJECT AREA : Party
    SOURCE: BENE_DATA_VW
    Teradata Source Code: 50
    Description: Initial load for rel_party_agreement table
    JIRA: TERSUN-3431
    Create Date:2021-08-27

    ===============================================================================================================
    Version/JIRA Story#     Created By     Last_Modified_Date   Description
    ---------------------------------------------------------------------------------------------------------------
    JIRA 3390              Party-Tier2            08/18       First Version of DML for Tier-2
    JIRA 3817              Party-Tier2            12/23       Removed src_del_ind while calculating CURR_IND
												  05/31
    ------------------------------------------------------------------------------------------------------------------
    */

/* truncate Work table */

TRUNCATE TABLE EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT;	

-- Collect Statistics on source table for improving performance

SELECT ANALYZE_STATISTICS('PROD_STND_VW_TERSUN.BEN_DATA_VW');


DROP TABLE IF EXISTS TEMP_BENE_DATA_VW;

/* LOAD CLEANSED DATA IN TEMP TABLE */

CREATE LOCAL TEMPORARY TABLE TEMP_BENE_DATA_VW ON COMMIT PRESERVE ROWS AS 
SELECT 
    CLEAN_STRING(CARR_ADMIN_SYS_CD)                                         AS CARR_ADMIN_SYS_CD,
    UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_PFX), 20, '0', TRUE) 	            AS HLDG_KEY_PFX,
    LPAD(CLEAN_STRING(HLDG_KEY)::VARCHAR, 20, '0') 			                AS HLDG_KEY,
    UDF_ISNUM_LPAD(CLEAN_STRING(HLDG_KEY_SFX), 20, '0', TRUE) 		        AS HLDG_KEY_SFX,
    CLEAN_STRING(VOLTAGEACCESS(BEN_FRST_NM,'name')) 		                AS BEN_FRST_NM,
    CLEAN_STRING(VOLTAGEACCESS(BEN_MDL_NM,'name'))		                    AS BEN_MDL_NM,
    CLEAN_STRING(VOLTAGEACCESS(BEN_LST_NM,'name'))		                    AS BEN_LST_NM,
	CLEAN_STRING(VOLTAGEACCESS(BEN_PFX_NM,'name'))		AS BEN_PFX_NM,
	CLEAN_STRING(VOLTAGEACCESS(BEN_SFX_NM,'name'))		AS BEN_SFX_NM,
    CLEAN_STRING(VOLTAGEACCESS(BEN_ARGMT_TXT,'freeform')) 	                AS BEN_ARGMT_TXT,
    CLEAN_STRING(BEN_ROW_CNTR_CD)	                                        AS BEN_ROW_CNTR_CD,
    COALESCE(CLEAN_STRING(RLE_CD),'Bene')			                        AS RLE_CD,
    CLEAN_STRING(RLE_STYP_CD)		                                        AS RLE_STYP_CD,
    UUID_GEN(NULL)::UUID                                                    AS DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
    COALESCE(BEN_DATA_FR_DT,'01/01/0001')::DATE                             AS BEGIN_DT,
    COALESCE(BEN_DATA_FR_DT,'01/01/0001')::TIMESTAMP(6)                     AS BEGIN_DTM,
    CURRENT_TIMESTAMP(6) 			                                        AS ROW_PROCESS_DTM,
    FALSE 							                                        AS LOGICAL_DELETE_IND,
    CASE WHEN CLEAN_STRING(CURR_IND)='Y'
        THEN TRUE 
        ELSE FALSE END 				AS CURRENT_ROW_IND,
	CASE WHEN CLEAN_STRING(CURR_IND)='Y' THEN '9999-12-31'
		 WHEN CLEAN_STRING(CURR_IND)='N' AND BEN_DATA_TO_DT::DATE  = '9999-12-31'::DATE THEN CURRENT_TIMESTAMP(6)::DATE 
		 WHEN BEN_DATA_TO_DT IS NULL THEN CURRENT_TIMESTAMP(6)::DATE 
		 ELSE BEN_DATA_TO_DT::DATE  END AS END_DT,
	CASE WHEN CLEAN_STRING(CURR_IND)='Y' THEN '9999-12-31'::TIMESTAMP(6)
		 WHEN CLEAN_STRING(CURR_IND)='N' AND BEN_DATA_TO_DT::DATE  = '9999-12-31'::DATE THEN CURRENT_TIMESTAMP(6) 
		 WHEN BEN_DATA_TO_DT IS NULL THEN CURRENT_TIMESTAMP(6)
		 ELSE BEN_DATA_TO_DT END AS END_DTM,
    '238' 							                                        AS SOURCE_SYSTEM_ID,
    FALSE 							                                        AS RESTRICTED_ROW_IND,
    UUID_GEN(NULL)::UUID                                                    AS DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
    NULL                                                                    AS ADDRESS_TYPE_CDE,
    NULL                                                                    AS ATTENTION_LINE_TXT,
    UUID_GEN(NULL)::UUID                                                    AS DIM_PHONE_NATURAL_KEY_HASH_UUID,
    UUID_GEN(NULL)::UUID                                                    AS DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
    UUID_GEN(NULL)::UUID                                                    AS DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
    NULL::INT                                                               AS BENEFICIARY_SUB_CLASS_NR,
    CLEAN_STRING(BEN_REL_TXT)                                               AS BENEFICIARY_RELATIONSHIP_NM,
    BEN_PCT                                                                 AS BENEFICIARY_ALLOCATION_PCT,
    NULL::NUMERIC                                                           AS BENEFICIARY_ALLOCATION_AMT,
    SUBSTRING(CLEAN_STRING(VOLTAGEACCESS(BEN_ARGMT_TXT,'freeform')),1,500)  AS BENEFICIARY_AGREEMENT_TXT,
    NULL                                                                    AS SOURCE_PARTY_ROLE_CDE,
    NULL                                                                    AS SOURCE_PARTY_SUB_ROLE_CDE,
    NULL                                                                    AS SOURCE_ADDRESS_TYPE_CDE,
    CASE WHEN CLEAN_STRING(SRC_DEL_IND)='Y'
		THEN TRUE
		ELSE FALSE END 				AS SOURCE_DELETE_IND,
    NULL                                                                    AS BENEFICIARY_ISS_PER_STIRPES_CDE,
    COALESCE(BEN_EFF_DT,BEN_DATA_FR_DT::DATE,'01/01/0001')::DATE                  AS BUSINESS_STRT_DT,
    COALESCE(BEN_DATA_TO_DT::DATE,'9999-12-31')::DATE                             AS BUSINESS_END_DT,
    NULL::NUMERIC                                                           AS ADVISOR_FIRST_YEAR_COMMISSION,
    NULL::NUMERIC                                                           AS ADVISOR_RENEWAL_COMMISSION,
    UUID_GEN(NULL)::UUID                                                    AS ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
    UUID_GEN(NULL)::UUID                                                    AS FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
    NULL::BOOLEAN                                                           AS SOLICIT_WRITING_AGENT_IND,
    NULL::BOOLEAN                                                           AS SERVICE_AGENT_IND,
    NULL::BOOLEAN                                                           AS CAREER_CORPORATION_IND,
    NULL                                                                    AS SOURCE_PACKAGE_ID,
    NULL                                                                    AS SOURCE_STATUS_CDE,
    NULL                                                                    AS SOURCE_STATUS_REASON_CDE,
    NULL                                                                    AS AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
    NULL::INT                                                               AS ADVISOR_ORDER_NR,
    UUID_GEN(NULL)::UUID                                                    AS AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	UUID_GEN(NULL)::UUID			                                        AS DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	NULL::NUMERIC					                                        AS ADVISOR_COMMISSION_PCT,
    CLEAN_STRING(RLE_POS_CD)                                                AS BENEFICIARY_ROLE_POSITION_CDE,
    COALESCE(CLEAN_STRING(SRC_RLE_CD),'Bene')                               AS SOURCE_BENEFICIARY_ROLE_CDE,
    CLEAN_STRING(SRC_RLE_POS_CD)                                            AS SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
    CLEAN_STRING(RLE_SEQ_NBR)  AS BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
    CLEAN_STRING(SRC_RLE_SEQ_NBR)   AS SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	CASE WHEN BEN_EFF_DT IS NULL THEN '0001-01-01' ELSE BEN_EFF_DT::DATE END AS BENEFICIARY_EFFECTIVE_DT,
	CASE WHEN BEN_TRST_DT IS NULL THEN '0001-01-01' ELSE BEN_TRST_DT::DATE END AS BENEFICIARY_TRUST_DT,
    CLEAN_STRING(BEN_SPEC_DSGNTN_CD)                                        AS BENEFICIARY_SPECIAL_DESTINATION_CDE,
    CLEAN_STRING(BEN_ROW_CNTR_CD)                                           AS BENEFICIARY_ROW_CONTROL_CDE,
    BENE_AMT                                                                AS BENEFICIARY_DESTINATION_AMT,
    COALESCE(CLEAN_STRING(BEN_CLS_CD),'Unk')                                AS BENEFICIARY_CLASS_CDE,
    COALESCE(CLEAN_STRING(PROF_DSGN_CD),'Unk')                              AS BENEFICIARY_DESIGNATION_TXT,
	CLEAN_STRING(NM_FRMT_CD)                                                AS BENEFICIARY_NAME_FORMAT_CDE
    FROM
    PROD_STND_VW_TERSUN.BEN_DATA_VW
        WHERE SRC_SYS_ID='50' 
    ORDER BY 
    CARR_ADMIN_SYS_CD,
    HLDG_KEY_PFX,
    HLDG_KEY,
    HLDG_KEY_SFX,
    BEN_FRST_NM,
    BEN_MDL_NM,
    BEN_LST_NM,
    BEN_ARGMT_TXT,
    BEN_ROW_CNTR_CD,
    RLE_CD,
    RLE_STYP_CD, 
    BENEFICIARY_SUB_CLASS_NR ;

   
COMMIT;


DROP TABLE IF EXISTS INTERMEDIATE_TABLE_1;

/* CALCULATE UUID AND CHECK_SUM FIELDS */

CREATE LOCAL TEMPORARY TABLE INTERMEDIATE_TABLE_1 ON COMMIT PRESERVE ROWS AS
SELECT 
UUID_GEN(CARR_ADMIN_SYS_CD,CLEAN_STRING('IPA'),HLDG_KEY_PFX,HLDG_KEY,HLDG_KEY_SFX)::UUID                                             AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
UUID_GEN( COALESCE(CARR_ADMIN_SYS_CD,'')||COALESCE(HLDG_KEY_PFX,'')||COALESCE(HLDG_KEY,'')||COALESCE(HLDG_KEY_SFX,'')||COALESCE(BEN_ROW_CNTR_CD,'') )::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID,
UUID_GEN(RLE_CD)::UUID                                                                                                               AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
UUID_GEN('Unk')::UUID                                                                     AS REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
BEGIN_DT,
BEGIN_DTM,
ROW_PROCESS_DTM,
LOGICAL_DELETE_IND,
CURRENT_ROW_IND,
END_DT,
END_DTM,
SOURCE_SYSTEM_ID,
RESTRICTED_ROW_IND,
DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
ADDRESS_TYPE_CDE,
ATTENTION_LINE_TXT,
DIM_PHONE_NATURAL_KEY_HASH_UUID,
DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
BENEFICIARY_SUB_CLASS_NR,
BENEFICIARY_RELATIONSHIP_NM,
BENEFICIARY_ALLOCATION_PCT,
BENEFICIARY_ALLOCATION_AMT,
BENEFICIARY_AGREEMENT_TXT,
SOURCE_PARTY_ROLE_CDE,
SOURCE_PARTY_SUB_ROLE_CDE,
SOURCE_ADDRESS_TYPE_CDE,
SOURCE_DELETE_IND,
BENEFICIARY_ISS_PER_STIRPES_CDE,
BUSINESS_STRT_DT,
BUSINESS_END_DT,
ADVISOR_FIRST_YEAR_COMMISSION,
ADVISOR_RENEWAL_COMMISSION,
ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
SOLICIT_WRITING_AGENT_IND,
SERVICE_AGENT_IND,
CAREER_CORPORATION_IND,
SOURCE_PACKAGE_ID,
SOURCE_STATUS_CDE,
SOURCE_STATUS_REASON_CDE,
AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
ADVISOR_ORDER_NR,
AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
ADVISOR_COMMISSION_PCT,
BENEFICIARY_ROLE_POSITION_CDE,
SOURCE_BENEFICIARY_ROLE_CDE,
SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
BENEFICIARY_EFFECTIVE_DT,
BENEFICIARY_TRUST_DT,
BENEFICIARY_SPECIAL_DESTINATION_CDE,
BENEFICIARY_ROW_CONTROL_CDE,
BENEFICIARY_DESTINATION_AMT,
BENEFICIARY_CLASS_CDE,
BENEFICIARY_DESIGNATION_TXT,
BENEFICIARY_NAME_FORMAT_CDE,
CASE WHEN BENEFICIARY_NAME_FORMAT_CDE = 'U' 
	   AND BEN_FRST_NM IS NULL  
	   AND BEN_MDL_NM IS NULL 
	   AND BEN_LST_NM IS NULL 
	   AND BEN_SFX_NM IS NULL 
	   AND BEN_PFX_NM IS NULL 
	  THEN NULL
     WHEN BENEFICIARY_NAME_FORMAT_CDE = 'U' AND 
	     ( BEN_FRST_NM IS NOT NULL  
        OR BEN_MDL_NM IS NOT NULL 
        OR BEN_LST_NM IS NOT NULL 
        OR BEN_SFX_NM IS NOT NULL 
        OR BEN_PFX_NM IS NOT NULL )
      THEN ( 
	         COALESCE(BEN_PFX_NM,'') || COALESCE(BEN_FRST_NM,'') || 
			 COALESCE(BEN_MDL_NM,'') || COALESCE(BEN_LST_NM,'') || COALESCE(BEN_SFX_NM,'')
			) 
ELSE NULL END AS BENEFICIARY_UNFORMATTED_NM
FROM
    TEMP_BENE_DATA_VW
ORDER BY 
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
DIM_PARTY_NATURAL_KEY_HASH_UUID,
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
BENEFICIARY_SUB_CLASS_NR ;


COMMIT;


DROP TABLE IF EXISTS INTERMEDIATE_TABLE_2;

/* CALCULATING ROW_NUM FOR IDENTIFYING CORRECT END_DATES */

CREATE LOCAL TEMPORARY TABLE INTERMEDIATE_TABLE_2 ON COMMIT PRESERVE ROWS AS 
SELECT 
	DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_NATURAL_KEY_HASH_UUID,
	REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
	REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
	DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
	BEGIN_DT,
	BEGIN_DTM,
	ROW_PROCESS_DTM,
	LOGICAL_DELETE_IND,
	uuid_gen(source_delete_ind, beneficiary_relationship_nm, beneficiary_allocation_pct,VOLTAGEPROTECT(BENEFICIARY_AGREEMENT_TXT,'freeform'), beneficiary_role_position_cde,
		 source_beneficiary_role_cde, source_beneficiary_role_position_cde, beneficiary_role_sequence_nr_txt, 
		 source_beneficiary_role_sequence_nr_txt, beneficiary_effective_dt, beneficiary_trust_dt, beneficiary_special_destination_cde,
		 beneficiary_row_control_cde, beneficiary_class_cde, beneficiary_designation_txt, beneficiary_name_format_cde,VOLTAGEPROTECT(BENEFICIARY_UNFORMATTED_NM,'name'))::uuid		AS		CHECK_SUM,
	CURRENT_ROW_IND,
	END_DT,
	END_DTM,
	SOURCE_SYSTEM_ID,
	RESTRICTED_ROW_IND,
	DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
	ADDRESS_TYPE_CDE,
	ATTENTION_LINE_TXT,
	DIM_PHONE_NATURAL_KEY_HASH_UUID,
	DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
	DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
	BENEFICIARY_SUB_CLASS_NR,
	BENEFICIARY_RELATIONSHIP_NM,
	BENEFICIARY_ALLOCATION_PCT,
	BENEFICIARY_ALLOCATION_AMT,
	VOLTAGEPROTECT(BENEFICIARY_AGREEMENT_TXT,'freeform') AS BENEFICIARY_AGREEMENT_TXT,
	SOURCE_PARTY_ROLE_CDE,
	SOURCE_PARTY_SUB_ROLE_CDE,
	SOURCE_ADDRESS_TYPE_CDE,
	SOURCE_DELETE_IND,
	BENEFICIARY_ISS_PER_STIRPES_CDE,
	BUSINESS_STRT_DT,
	BUSINESS_END_DT,
	ADVISOR_FIRST_YEAR_COMMISSION,
	ADVISOR_RENEWAL_COMMISSION,
	ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
	SOLICIT_WRITING_AGENT_IND,
	SERVICE_AGENT_IND,
	CAREER_CORPORATION_IND,
	SOURCE_PACKAGE_ID,
	SOURCE_STATUS_CDE,
	SOURCE_STATUS_REASON_CDE,
	AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
	ADVISOR_ORDER_NR,
	AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
	ADVISOR_COMMISSION_PCT,
	BENEFICIARY_ROLE_POSITION_CDE,
	SOURCE_BENEFICIARY_ROLE_CDE,
	SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
	BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
	BENEFICIARY_EFFECTIVE_DT,
	BENEFICIARY_TRUST_DT,
	BENEFICIARY_SPECIAL_DESTINATION_CDE,
	BENEFICIARY_ROW_CONTROL_CDE,
	BENEFICIARY_DESTINATION_AMT,
	BENEFICIARY_CLASS_CDE,
	BENEFICIARY_DESIGNATION_TXT,
	BENEFICIARY_NAME_FORMAT_CDE,
	VOLTAGEPROTECT(BENEFICIARY_UNFORMATTED_NM,'name') AS BENEFICIARY_UNFORMATTED_NM,
	ROW_NUMBER() OVER(PARTITION BY A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,A.DIM_PARTY_NATURAL_KEY_HASH_UUID,
	A.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,A.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID ORDER BY A.END_DTM DESC,A.BEGIN_DTM DESC) RW_NUM
 FROM INTERMEDIATE_TABLE_1 A
ORDER BY A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,A.DIM_PARTY_NATURAL_KEY_HASH_UUID,
A.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,A.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,A.END_DTM,A.BEGIN_DTM ;


COMMIT;


 
/* INSERTING DATA IN WORK TABLE AND CALCULATE END DATES FOR INCOMING DATA */

create local temporary table work1 on commit preserve rows as
select * from EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT where 1<>1;
INSERT INTO work1 --EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT 
(
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
DIM_PARTY_NATURAL_KEY_HASH_UUID,
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
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
DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
ADDRESS_TYPE_CDE,
ATTENTION_LINE_TXT,
DIM_PHONE_NATURAL_KEY_HASH_UUID,
DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
BENEFICIARY_SUB_CLASS_NR,
BENEFICIARY_RELATIONSHIP_NM,
BENEFICIARY_ALLOCATION_PCT,
BENEFICIARY_ALLOCATION_AMT,
BENEFICIARY_AGREEMENT_TXT,
SOURCE_PARTY_ROLE_CDE,
SOURCE_PARTY_SUB_ROLE_CDE,
SOURCE_ADDRESS_TYPE_CDE,
SOURCE_DELETE_IND,
BENEFICIARY_ISS_PER_STIRPES_CDE,
BUSINESS_STRT_DT,
BUSINESS_END_DT,
ADVISOR_FIRST_YEAR_COMMISSION,
ADVISOR_RENEWAL_COMMISSION,
ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
SOLICIT_WRITING_AGENT_IND,
SERVICE_AGENT_IND,
CAREER_CORPORATION_IND,
SOURCE_PACKAGE_ID,
SOURCE_STATUS_CDE,
SOURCE_STATUS_REASON_CDE,
AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
ADVISOR_ORDER_NR,
AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
ADVISOR_COMMISSION_PCT,
BENEFICIARY_ROLE_POSITION_CDE,
SOURCE_BENEFICIARY_ROLE_CDE,
SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
BENEFICIARY_EFFECTIVE_DT,
BENEFICIARY_TRUST_DT,
BENEFICIARY_SPECIAL_DESTINATION_CDE,
BENEFICIARY_ROW_CONTROL_CDE,
BENEFICIARY_DESTINATION_AMT,
BENEFICIARY_CLASS_CDE,
BENEFICIARY_DESIGNATION_TXT,
BENEFICIARY_NAME_FORMAT_CDE,
BENEFICIARY_UNFORMATTED_NM
)
SELECT
A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
A.DIM_PARTY_NATURAL_KEY_HASH_UUID,
A.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
A.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
A.DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
A.BEGIN_DT,
A.BEGIN_DTM,
A.ROW_PROCESS_DTM,
:audit_id               AS AUDIT_ID,
A.LOGICAL_DELETE_IND,
A.CHECK_SUM,
CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 THEN FALSE ELSE A.CURRENT_ROW_IND END AS CURRENT_ROW_IND,
CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND  A.END_DT > B.BEGIN_DT - INTERVAL '1' DAY THEN B.BEGIN_DT - INTERVAL '1' DAY 
      WHEN B.RW_NUM IS NULL THEN '9999-12-31'::DATE 
      ELSE A.END_DT END AS END_DT,
CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND  A.END_DTM > B.BEGIN_DTM - INTERVAL '1' SECOND THEN B.BEGIN_DTM - INTERVAL '1' SECOND 
      WHEN B.RW_NUM IS NULL THEN '9999-12-31'::TIMESTAMP(6) 
      ELSE A.END_DTM END AS END_DTM,
A.SOURCE_SYSTEM_ID,
A.RESTRICTED_ROW_IND,
:audit_id                                                                                                              AS  UPDATE_AUDIT_ID,
A.DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
A.ADDRESS_TYPE_CDE,
A.ATTENTION_LINE_TXT,
A.DIM_PHONE_NATURAL_KEY_HASH_UUID,
A.DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
A.DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
A.BENEFICIARY_SUB_CLASS_NR,
A.BENEFICIARY_RELATIONSHIP_NM,
A.BENEFICIARY_ALLOCATION_PCT,
A.BENEFICIARY_ALLOCATION_AMT,
A.BENEFICIARY_AGREEMENT_TXT,
A.SOURCE_PARTY_ROLE_CDE,
A.SOURCE_PARTY_SUB_ROLE_CDE,
A.SOURCE_ADDRESS_TYPE_CDE,
A.SOURCE_DELETE_IND,
A.BENEFICIARY_ISS_PER_STIRPES_CDE,
A.BUSINESS_STRT_DT,
CASE WHEN A.RW_NUM<B.RW_NUM AND A.RW_NUM=B.RW_NUM-1 AND  A.BUSINESS_END_DT > B.BUSINESS_STRT_DT - INTERVAL '1' SECOND THEN B.BUSINESS_STRT_DT - INTERVAL '1' SECOND 
      WHEN B.RW_NUM IS NULL THEN '9999-12-31'::TIMESTAMP(6) 
      ELSE A.BUSINESS_END_DT END AS BUSINESS_END_DT,                                                                               
A.ADVISOR_FIRST_YEAR_COMMISSION,
A.ADVISOR_RENEWAL_COMMISSION,
A.ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
A.FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
A.SOLICIT_WRITING_AGENT_IND,
A.SERVICE_AGENT_IND,
A.CAREER_CORPORATION_IND,
A.SOURCE_PACKAGE_ID,
A.SOURCE_STATUS_CDE,
A.SOURCE_STATUS_REASON_CDE,
A.AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
A.ADVISOR_ORDER_NR,
A.AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
A.DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
A.ADVISOR_COMMISSION_PCT,
A.BENEFICIARY_ROLE_POSITION_CDE,
A.SOURCE_BENEFICIARY_ROLE_CDE,
A.SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
A.BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
A.SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
A.BENEFICIARY_EFFECTIVE_DT,
A.BENEFICIARY_TRUST_DT,
A.BENEFICIARY_SPECIAL_DESTINATION_CDE,
A.BENEFICIARY_ROW_CONTROL_CDE,
A.BENEFICIARY_DESTINATION_AMT,
A.BENEFICIARY_CLASS_CDE,
A.BENEFICIARY_DESIGNATION_TXT,
A.BENEFICIARY_NAME_FORMAT_CDE,
A.BENEFICIARY_UNFORMATTED_NM
FROM
INTERMEDIATE_TABLE_2 A
LEFT OUTER JOIN
INTERMEDIATE_TABLE_2 B
ON 
A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID=B.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID AND
A.DIM_PARTY_NATURAL_KEY_HASH_UUID=B.DIM_PARTY_NATURAL_KEY_HASH_UUID AND
A.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID=B.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID AND
A.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID=B.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID  AND 
A.RW_NUM=B.RW_NUM-1
ORDER BY A.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,A.DIM_PARTY_NATURAL_KEY_HASH_UUID,
A.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,A.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID ;


COMMIT;

/* DROPPING TEMPORARY TABLES */
DROP TABLE IF EXISTS TEMP_BENE_DATA_VW;
DROP TABLE IF EXISTS INTERMEDIATE_TABLE_1;
DROP TABLE IF EXISTS INTERMEDIATE_TABLE_2;

SELECT ANALYZE_STATISTICS('EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT');


DELETE FROM EDW.REL_PARTY_AGREEMENT WHERE SOURCE_SYSTEM_ID IN ('238','50') AND SOURCE_BENEFICIARY_ROLE_CDE='Bene' ;

/* INSERTING INTO TARGET TABLE */

INSERT INTO EDW.REL_PARTY_AGREEMENT 
(
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
DIM_PARTY_NATURAL_KEY_HASH_UUID,
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
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
DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
ADDRESS_TYPE_CDE,
ATTENTION_LINE_TXT,
DIM_PHONE_NATURAL_KEY_HASH_UUID,
DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
BENEFICIARY_SUB_CLASS_NR,
BENEFICIARY_RELATIONSHIP_NM,
BENEFICIARY_ALLOCATION_PCT,
BENEFICIARY_ALLOCATION_AMT,
BENEFICIARY_AGREEMENT_TXT,
SOURCE_PARTY_ROLE_CDE,
SOURCE_PARTY_SUB_ROLE_CDE,
SOURCE_ADDRESS_TYPE_CDE,
SOURCE_DELETE_IND,
BENEFICIARY_ISS_PER_STIRPES_CDE,
BUSINESS_STRT_DT,
BUSINESS_END_DT,
ADVISOR_FIRST_YEAR_COMMISSION,
ADVISOR_RENEWAL_COMMISSION,
ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
SOLICIT_WRITING_AGENT_IND,
SERVICE_AGENT_IND,
CAREER_CORPORATION_IND,
SOURCE_PACKAGE_ID,
SOURCE_STATUS_CDE,
SOURCE_STATUS_REASON_CDE,
AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
ADVISOR_ORDER_NR,
AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
ADVISOR_COMMISSION_PCT,
BENEFICIARY_ROLE_POSITION_CDE,
SOURCE_BENEFICIARY_ROLE_CDE,
SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
BENEFICIARY_EFFECTIVE_DT,
BENEFICIARY_TRUST_DT,
BENEFICIARY_SPECIAL_DESTINATION_CDE,
BENEFICIARY_ROW_CONTROL_CDE,
BENEFICIARY_DESTINATION_AMT,
BENEFICIARY_CLASS_CDE,
BENEFICIARY_DESIGNATION_TXT,
BENEFICIARY_NAME_FORMAT_CDE,
BENEFICIARY_UNFORMATTED_NM
)
SELECT
DIM_AGREEMENT_NATURAL_KEY_HASH_UUID,
DIM_PARTY_NATURAL_KEY_HASH_UUID,
REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID,
REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,
DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
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
DIM_ADDRESS_NATURAL_KEY_HASH_UUID,
ADDRESS_TYPE_CDE,
ATTENTION_LINE_TXT,
DIM_PHONE_NATURAL_KEY_HASH_UUID,
DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID,
DIM_PARTY_AKA_NAME_NATURAL_KEY_HASH_UUID,
BENEFICIARY_SUB_CLASS_NR,
BENEFICIARY_RELATIONSHIP_NM,
BENEFICIARY_ALLOCATION_PCT,
BENEFICIARY_ALLOCATION_AMT,
BENEFICIARY_AGREEMENT_TXT,
SOURCE_PARTY_ROLE_CDE,
SOURCE_PARTY_SUB_ROLE_CDE,
SOURCE_ADDRESS_TYPE_CDE,
SOURCE_DELETE_IND,
BENEFICIARY_ISS_PER_STIRPES_CDE,
BUSINESS_STRT_DT,
BUSINESS_END_DT,
ADVISOR_FIRST_YEAR_COMMISSION,
ADVISOR_RENEWAL_COMMISSION,
ADVISOR_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
FIRM_DIM_SELLING_AGREEMENT_NATURAL_KEY_HASH_UUID,
SOLICIT_WRITING_AGENT_IND,
SERVICE_AGENT_IND,
CAREER_CORPORATION_IND,
SOURCE_PACKAGE_ID,
SOURCE_STATUS_CDE,
SOURCE_STATUS_REASON_CDE,
AGREEMENT_DISTRIBUTION_CHANNEL_CDE,
ADVISOR_ORDER_NR,
AGENCY_DIM_PARTY_NATURAL_KEY_HASH_UUID,
DISTRIBUTOR_DIM_PARTY_NATURAL_KEY_HASH_UUID,
ADVISOR_COMMISSION_PCT,
BENEFICIARY_ROLE_POSITION_CDE,
SOURCE_BENEFICIARY_ROLE_CDE,
SOURCE_BENEFICIARY_ROLE_POSITION_CDE,
BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
SOURCE_BENEFICIARY_ROLE_SEQUENCE_NR_TXT,
BENEFICIARY_EFFECTIVE_DT,
BENEFICIARY_TRUST_DT,
BENEFICIARY_SPECIAL_DESTINATION_CDE,
BENEFICIARY_ROW_CONTROL_CDE,
BENEFICIARY_DESTINATION_AMT,
BENEFICIARY_CLASS_CDE,
BENEFICIARY_DESIGNATION_TXT,
BENEFICIARY_NAME_FORMAT_CDE,
BENEFICIARY_UNFORMATTED_NM
FROM
EDW_WORK.PARTY_CDALIFCOMLIFE_REL_PARTY_AGREEMENT;

COMMIT;

SELECT ANALYZE_STATISTICS('EDW.REL_PARTY_AGREEMENT');