select c.data_type, count(*) from 
 information_schema.columns  c
 where ( table_schema  ='h1b_analysis_stg')
 and table_name not like  'test%'
 group by data_type;


select concat( ' " truncate table ',table_schema ,'.', table_name , ';" ') from information_schema.tables 
where (table_type = upper('base table') and table_schema  ='h1b_analysis_stg')
and table_name not like  'test%'
 ;



/* data type preference for Spark*/
select 
 table_name  , 
 string_agg(cast_column,'') as out
 from 
 (select concat(table_schema,'.',table_name) table_name , 
 concat ( 'cast ( ',column_name,' as ',case when c.data_type in ('character varying','text') then 'string'
 when  c.data_type in ('bigint') then  'Long'
 when c.data_type in ('int') then 'int'
 when c.data_type in ('numeric') then 'decimal'
 else c.data_type 
 end , 
 ' ) as ', column_name ,',') cast_column 
 from  information_schema.columns  c
 where ( table_schema  ='h1b_analysis_stg')
 and table_name not like  'test%') ls 
 group by table_name 
 order by 1 asc 
 ;






 update 
h1b_analysis_stg.stg_h1b_data_second h
set lat_test  =  m.lat,
lon_test = m.lon
from 
(select 
case_status,employer_name,soc_name,job_title,full_time_position,prevailing_wage,"YEAR",worksite,
case when lon = 'NA' then null else lon end::numeric lon ,case when lat = 'NA' then null else lat end::numeric lat
from  h1b_analysis_stg.stg_h1b_data_second ) m
where 
h.case_status =  m.case_status
and h.employer_name = m.employer_name
and h.soc_name = m.soc_name
and h.job_title = m.job_title
and h.full_time_position = m.full_time_position
and h.prevailing_wage = m.prevailing_wage
and h."YEAR" = m."YEAR"
and h.worksite = m.worksite
;

select concat('select ''',table_schema, '.', table_name,''' as table_name,count(*) as table_row_count from ',table_schema, '.', table_name,' union all')
from 
information_schema.tables 
where (table_type = upper('base table') and table_schema  ='h1b_analysis_stg')
and table_name not like  'test%'
;
