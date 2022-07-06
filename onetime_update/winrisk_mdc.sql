update edap_mdc.etl_project_staging_table
set pk_columns='policy_number,case_reqt_id,requirement_status_code',updated_on=current_timestamp(6)
where lower(staging_table_name)='winrisk_edw_winrisk_requirements';

commit;
