update edap_mdc.etl_project_staging_table set
pk_columns='bene_row_adm_sys_name,bene_row_ctrt_prefix,bene_row_ctrt_suffix,bene_row_ctrt_no,bene_row_cntr'
where upper(staging_table_name) = 'CDA_LIFCOM_LIFE_EDW_BENE_DELTA';


update edap_mdc.etl_project_staging_table set
pk_columns='bene_row_adm_sys_name,bene_row_ctrt_prefix,bene_row_ctrt_suffix,bene_row_ctrt_no,bene_row_cntr'
where upper(staging_table_name) = 'CDA_LVRGVL_EDW_BENE_DELTA';


update edap_mdc.etl_project_staging_table set
pk_columns='bene_row_adm_sys_name,bene_row_ctrt_prefix,bene_row_ctrt_suffix,bene_row_ctrt_no,bene_row_cntr'
where upper(staging_table_name) = 'CDA_VNTG1_LIFE_EDW_BENE_DELTA';


update edap_mdc.etl_project_staging_table set
pk_columns='BENE_ROW_ADM_SYS_NAME,BENE_ROW_CTRT_PREFIX,BENE_ROW_CTRT_SUFFIX,bene_row_ctrt_no,bene_row_cntr'
where staging_table_name = 'cda_mpr_life_edw_bene_delta' ;