drop table if exists rel_contact;

create local temporary table rel_contact on commit preserve rows AS 
select * from edw_vw.rel_contact_preference_vw; --4013108

select distinct(source_system_id) from edw_vw.rel_contact_preference_vw;

drop table if exists dim_phone;

create local temporary table dim_phone on commit preserve rows AS 
select * from edw_vw.dim_phone_vw; --2982980

drop table if exists bad_records;

create local temporary table bad_records on commit preserve rows as
select distinct rcp.dim_phone_natural_key_hash_uuid
from rel_contact rcp
left join dim_phone prd
on rcp.dim_phone_natural_key_hash_uuid = prd.dim_phone_natural_key_hash_uuid
and prd.current_row_ind = True
where prd.dim_phone_natural_key_hash_uuid is null;

select * from bad_records

update rel_contact p
set current_row_ind='False',logical_delete_ind='True',row_process_dtm=CURRENT_TIMESTAMP(6) 
from(
select dim_phone_natural_key_hash_uuid from bad_records
)q
where p.dim_phone_natural_key_hash_uuid=q.dim_phone_natural_key_hash_uuid
and current_row_ind;
--Updated Rows	1636920(when current row_ind is true)
--Updated Rows 217332(when current_row_ind is false)  
--Updated Rows 1863342(when current_row_ind and logical delete are ignored) 
--Updated Rows	9090(when current_row_ind is false and logical delete_ind is false)


--checking dim_phone_natural_key_hash_uuid in rel_contact where it has mutliple records
select dim_phone_natural_key_hash_uuid,count(dim_phone_natural_key_hash_uuid) from rel_contact
where  
dim_phone_natural_key_hash_uuid in (select dim_phone_natural_key_hash_uuid from bad_records)
group by dim_phone_natural_key_hash_uuid having count(dim_phone_natural_key_hash_uuid)>1;

/*
c8a98c6d-d7f7-e229-fde9-293ce54be5e0--2
c4e7ba32-7053-cc8e-5ba4-e9b3d81d6202--2
a9433bb2-333a-925b-6035-c0a6f91987cf--2
2c4f5c40-866f-54f4-4ef6-cf633348309f--1863305
*/

