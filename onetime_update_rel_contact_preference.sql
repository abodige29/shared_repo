drop table if exists bad_records;

create local temporary table bad_records on commit preserve rows as
select distinct rcp.dim_phone_natural_key_hash_uuid
from edw.rel_contact_preference rcp
left join edw.dim_phone prd
on rcp.dim_phone_natural_key_hash_uuid = prd.dim_phone_natural_key_hash_uuid
and prd.current_row_ind = True
where prd.dim_phone_natural_key_hash_uuid is null;


update edw.rel_contact_preference p
set current_row_ind='False',logical_delete_ind='True',row_process_dtm=current_timestamp(6) 
from(
select dim_phone_natural_key_hash_uuid from bad_records
)q
where p.dim_phone_natural_key_hash_uuid=q.dim_phone_natural_key_hash_uuid;