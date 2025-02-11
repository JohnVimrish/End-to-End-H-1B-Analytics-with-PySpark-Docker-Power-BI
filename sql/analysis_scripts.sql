select concat( ' " truncate table ',table_schema ,'.', table_name , ';" ') from information_schema.tables 
where (table_type = upper('base table') and table_schema  ='h1b_analysis_stg')
and table_name not like  'test%'
 ;
