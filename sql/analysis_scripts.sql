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



select c.data_type, count(*) from 
 information_schema.columns  c
 where ( table_schema  ='h1b_analysis_stg')
 and table_name not like  'test%'
 group by data_type