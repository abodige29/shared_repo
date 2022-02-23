select * from edw_rdm_vw.rdm_column_level_encryption_vw rclev where table_nm ilike 'NB_REQ_VW';
select * from edw_rdm_vw.rdm_column_level_encryption_vw rclev where table_nm ilike 
'prod_nbr_vw_tersun.NB_REQ_VW';

select * from edw_rdm_vw.rdm_column_level_encryption_vw rclev where table_nm='HIER_RGIS_VW';

select * from prod_nbr_vw_tersun.NB_REQ_VW limit 10;

select distinct hldg_key, hldg_key_pfx, hldg_key_sfx, carr_admin_sys_cd from prod_nbr_vw_tersun.NB_REQ_VW req  
left join prod_nbr_vw_tersun.NB_APPL_VW appl on req.appl_id = appl.appl_id


SELECT * FROM PROD_NBR_VW_TERSUN.NB_APPL_VW;

SELECT * FROM TEMP_PREWORK 
WHERE dim_agreement_natural_key_hash_uuid='242d452b-1850-261e-6efe-59ebd6351dc3' AND
ref_requirement_type_natural_key_hash_uuid='13da41a8-f513-281c-59c2-4bbcf87617c4' AND
requirement_case_id='Ann20160108b3' ;

SELECT RW_NUM,begin_dt,begin_dtm,end_dt,end_dtm,current_row_ind,CHECK_SUM FROM VT_ORDER_BY 
WHERE dim_agreement_natural_key_hash_uuid='242d452b-1850-261e-6efe-59ebd6351dc3' AND
ref_requirement_type_natural_key_hash_uuid='13da41a8-f513-281c-59c2-4bbcf87617c4' AND
requirement_case_id='Ann20160108b3' ORDER BY RW_NUM ;


SELECT * FROM  
WHERE dim_agreement_natural_key_hash_uuid='242d452b-1850-261e-6efe-59ebd6351dc3' AND
ref_requirement_type_natural_key_hash_uuid='13da41a8-f513-281c-59c2-4bbcf87617c4' AND
requirement_case_id='Ann20160108b3' ;

SELECT begin_dt,begin_dtm,end_dt,end_dtm,current_row_ind,CHECK_SUM  FROM   TEMP_PREWORK_A 
WHERE dim_agreement_natural_key_hash_uuid='242d452b-1850-261e-6efe-59ebd6351dc3' AND
ref_requirement_type_natural_key_hash_uuid='13da41a8-f513-281c-59c2-4bbcf87617c4' AND
requirement_case_id='Ann20160108b3' ;


