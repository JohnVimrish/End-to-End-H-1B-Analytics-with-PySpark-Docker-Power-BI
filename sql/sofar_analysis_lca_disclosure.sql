

SELECT pg_size_pretty( pg_database_size('H1b_Analysis') );




Try the Database Object Size Functions. An example:

SELECT pg_size_pretty(pg_total_relation_size('"<schema>"."<table>"'));

For all tables, something along the lines of:

SELECT
    table_schema || '.' || table_name AS table_full_name,
    pg_size_pretty(pg_total_relation_size('"' || table_schema || '"."' || table_name || '"')) AS size
FROM information_schema.tables 
where (table_type = 'BASE TABLE' and table_Schema  = 'h1b_analysis_stg')
and table_name not like  'test%'
ORDER BY
    pg_total_relation_size('"' || table_schema || '"."' || table_name || '"') DESC;


create schema h1b_analysis_sil ;

-- crime_data.v_calendar_time source

create or replace
view h1b_analysis_sil.v_calendar_time
as with date_time as (
select
	to_char(dates_series.dd::timestamp with time zone,
	'yyyymmdd'::text)::integer as date_key,
	dates_series.dd as calendar_date,
	dates_series.date_time as calendar_date_time,
	to_char(dates_series.dd::timestamp with time zone,
	'Day'::text) as day_name,
	extract(isodow
from
	dates_series.dd) as day_of_week,
	extract(day
from
	dates_series.dd) as day_of_month,
	extract(month
from
	dates_series.dd) as calendar_month,
	to_char(dates_series.dd::timestamp with time zone,
	'Month'::text) as month_name,
	to_char(dates_series.dd::timestamp with time zone,
	'Mon'::text) as month_name_abbr,
	extract(quarter
from
	dates_series.dd) as quarter_,
	concat('Q-',
	extract(quarter
from
	dates_series.dd)) as quarter_name,
	extract(year
from
	dates_series.dd) as calendar_year,
	date_trunc('month'::text,
	dates_series.dd::timestamp with time zone)::date as start_of_month,
	date_trunc('quarter'::text,
	dates_series.dd::timestamp with time zone)::date as start_of_quarter,
	date_trunc('YEAR'::text,
	dates_series.dd::timestamp with time zone)::date as start_of_year,
	(date_trunc('MONTH'::text,
	dates_series.dd::timestamp with time zone) + '1 mon -1 days'::interval)::date as end_of_month,
	(date_trunc('YEAR'::text,
	dates_series.dd::timestamp with time zone)::date + '1 year -1 days'::interval)::date as end_of_year,
	to_char(dates_series.dd::timestamp with time zone,
	'yyyymm'::text) as year_month,
	concat(extract(year
from
	dates_series.dd),
	'-',
	to_char(dates_series.dd::timestamp with time zone,
	'Mon'::text)) as year_month_name,
	case
		when extract(isodow
	from
		dates_series.dd) = any (array[6::numeric,
		7::numeric]) then 1
		else 0
	end as is_weekend
from
	(
	select
		ls.datum::timestamp(0) without time zone as date_time,
		ls.datum::date as dd
	from
		(
		select
			generate_series('2000-01-01'::date::timestamp with time zone,
			now(),
			'1 day'::interval) as datum) ls) dates_series
        ),
group_form as (
select
	ls.calendar_month,
	ls.start_of_year,
	ls.end_of_month,
	ls.end_of_year,
	ls.year_month,
	ls.calendar_year,
	lag(ls.start_of_month,
	1) over (
	order by ls.start_of_year,
	ls.start_of_month) as previous_month_start_date,
	lag(ls.end_of_month,
	1) over (
	order by ls.start_of_year,
	ls.start_of_month) as previous_month_end_date,
	lag(ls.end_of_month,
	13) over (
	order by ls.start_of_year,
	ls.start_of_month) as ttm_month_end_date
from
	(
	select
		dt_1.calendar_month,
		dt_1.start_of_year,
		dt_1.start_of_month,
		dt_1.end_of_month,
		dt_1.end_of_year,
		dt_1.year_month,
		dt_1.calendar_year,
		count(*) as count
	from
		date_time dt_1
	group by
		dt_1.calendar_month,
		dt_1.start_of_year,
		dt_1.start_of_month,
		dt_1.end_of_month,
		dt_1.end_of_year,
		dt_1.year_month,
		dt_1.calendar_year) ls
        ),
group_form_sec as (
select
	ls.start_of_year,
	ls.end_of_year,
	ls.calendar_year,
	lag(ls.start_of_year,
	1) over (
	order by ls.start_of_year) as previous_year_start_date,
	lag(ls.end_of_year,
	1) over (
	order by ls.start_of_year) as previous_year_end_date
from
	(
	select
		dt_1.start_of_year,
		dt_1.end_of_year,
		dt_1.calendar_year,
		count(*) as count
	from
		date_time dt_1
	group by
		dt_1.start_of_year,
		dt_1.end_of_year,
		dt_1.calendar_year) ls
        )
 select
	dt.date_key,
	dt.calendar_date,
	dt.calendar_date_time,
	dt.day_name,
	dt.day_of_week,
	dt.day_of_month,
	dt.calendar_month,
	dt.month_name,
	dt.month_name_abbr,
	dt.quarter_,
	dt.quarter_name,
	dt.calendar_year,
	dt.start_of_month,
	dt.start_of_quarter,
	dt.start_of_year,
	dt.end_of_month,
	dt.end_of_year,
	dt.year_month,
	dt.year_month_name,
	dt.is_weekend,
	gf.previous_month_start_date,
	gfs.previous_year_start_date,
	gf.previous_month_end_date,
	gfs.previous_year_end_date,
	gf.ttm_month_end_date
from
	date_time dt,
	group_form gf,
	group_form_sec gfs
where
	dt.calendar_month = gf.calendar_month
	and dt.start_of_year = gf.start_of_year
	and dt.end_of_month = gf.end_of_month
	and dt.end_of_year = gf.end_of_year
	and dt.year_month = gf.year_month
	and dt.calendar_year = gf.calendar_year
	and dt.start_of_year = gfs.start_of_year
	and dt.end_of_year = gfs.end_of_year
	and dt.calendar_year = gfs.calendar_year;



select date_key,count(*) from h1b_analysis_sil.v_calendar_time
group by date_key 
having count(*)> 1
;




select  case_number , case_status , count(*) 
from h1b_analysis_stg.stg_lca_disclosure_data
group by  case_number , case_status
having count(*) > 1;



select visa_class, count(*) from h1b_analysis_stg.stg_lca_disclosure_data
group by visa_class;

select * from h1b_analysis_stg. stg_lca_disclosure_data  
where lower(employer_name)  like '%biz%met%'
order by begin_date asc, end_Date asc;


select distinct case_status from  h1b_analysis_stg. stg_lca_disclosure_data  

drop    view h1b_analysis_sil.lca_disclosure_data ;
create or replace  view h1b_analysis_sil.lca_disclosure_data
as
select 
case_number,
case_status,
received_date,
vct.date_key petition_received_datekey ,
decision_date,
des.date_key descision_datekey ,
original_cert_date,
visa_class,
lca.job_title,
soc_code,
soc_title,
full_time_position,
begin_date,
beg.date_key term_begin_datekey ,
end_date,
en.date_key term_end_datekey ,
total_worker_positions,
new_employment,
continued_employment,
change_previous_employment,
new_concurrent_employment,
change_employer,
amended_petition,
employer_name,
trade_name_dba,
/*employer_address1,
employer_address2,*/
employer_city,
employer_state,
employer_postal_code,
employer_country,
/*employer_province,
employer_phone,
employer_phone_ext,*/
naics_code,
/*employer_poc_last_name,
employer_poc_first_name,
employer_poc_middle_name,
employer_poc_job_title,
employer_poc_address1,
employer_poc_address2,
employer_poc_city,
employer_poc_state,
employer_poc_postal_code,
employer_poc_country,
employer_poc_province,
employer_poc_phone,
employer_poc_phone_ext,
employer_poc_email,*/
/*agent_representing_employer,
agent_attorney_last_name,
agent_attorney_first_name,
agent_attorney_middle_name,
agent_attorney_address1,
agent_attorney_address2,
agent_attorney_city,
agent_attorney_state,
agent_attorney_postal_code,
agent_attorney_country,
agent_attorney_province,
agent_attorney_phone,
agent_attorney_phone_ext,
agent_attorney_email_address,
lawfirm_name_business_name,
state_of_highest_court,
name_of_highest_state_court,
worksite_workers,
secondary_entity,
secondary_entity_business_name,
worksite_address1,
worksite_address2,
worksite_city,
worksite_county,
worksite_state,
worksite_postal_code,*/
wage_rate_of_pay_from,
wage_rate_of_pay_to,
wage_unit_of_pay,
prevailing_wage,
pw_unit_of_pay,
pw_tracking_number,
pw_wage_level,
pw_oes_year,
pw_other_source,
pw_other_year,
/*pw_survey_publisher,
pw_survey_name,
total_worksite_locations,*/
agree_to_lc_statement,
h_1b_dependent,
willful_violator,
support_h1b,
statutory_basis,
appendix_a_attached,
/*public_disclosure,
preparer_last_name,
preparer_first_name,
preparer_middle_initial,
preparer_business_name,
preparer_email*/
coalesce(j.category,js.category) job_catergory 
from  h1b_analysis_stg. stg_lca_disclosure_data lca  
left join h1b_analysis_sil.calendar_time vct on lca.received_date  = vct.calendar_date 
left join h1b_analysis_sil.calendar_time des on lca.decision_date  = des.calendar_date
left join h1b_analysis_sil.calendar_time beg  on lca.begin_date  = beg.calendar_Date
left join h1b_analysis_sil.calendar_time en on lca.end_date  = en.calendar_Date 
left join  h1b_analysis_stg.updated_jobs j on j.column_name  = lca.job_title
left join  h1b_analysis_stg.job_catergory js on js.job_title  = lca.job_title
;

select type(calendar_year) from h1b_analysis_sil.calendar_time


select * from h1b_analysis_stg.stg_lca_disclosure_data ;
select count(*) from h1b_analysis_stg. stg_lca_disclosure_data

create index ind_lca_disclosure_dates  on h1b_analysis_stg. stg_lca_disclosure_data (received_date,decision_date,decision_date,begin_date,end_date)



select * from h1b_analysis_sil.calendar_time  


create  index ind_calendar_time on h1b_analysis_sil.calendar_time  (calendar_date) ;

-- h1b_analysis_sil.v_calendar_time source


select case_status,count(distinct case_number ) case_count  from h1b_analysis_stg.stg_lca_disclosure_data stg
where (new_employment  > 0 
and extract(year from stg.received_date)  = 2024)
and stg.continued_employment   =  0 
group by case_status ;



select * from h1b_analysis_stg.stg_lca_disclosure_data stg
where (new_employment  > 0 
and extract(year from stg.received_date)  = 2024)
and stg.continued_employment   =  0 ;




select distinct REGEXP_REPLACE(trim(sldd.job_title), '[="-]', '', 'g') job_title from h1b_analysis_stg.stg_lca_disclosure_data sldd ;

select * from h1b_analysis_stg.stg_lca_disclosure_data ls
h1b_analysis_stg.updated_jobs uj 
where ls.job

select  count(*) from 
h1b_analysis_stg.stg_lca_disclosure_data lca;

select lca.case_number ,lca.case_status,count(*)from h1b_analysis_stg.stg_lca_disclosure_data lca
left join  h1b_analysis_stg.updated_jobs j 
on j.column_name  = lca.job_title
left join  h1b_analysis_stg.job_catergory js
on js.job_title  = lca.job_title
--where 
--coalesce(j.column_name,js.job_title) is null  or coalesce(j.category,js.category) is null 
group by lca.case_number ,lca.case_status
having count(*)>1
;


select 
job_title from 
(select coalesce(j.column_name,js.job_title) ,coalesce(j.category,js.category), lca.job_title,count(*)from h1b_analysis_stg.stg_lca_disclosure_data lca
left join  h1b_analysis_stg.updated_jobs j 
on j.column_name  = lca.job_title
left join  h1b_analysis_stg.job_catergory js
on js.job_title  = lca.job_title
where 
coalesce(j.column_name,js.job_title) is null  or coalesce(j.category,js.category) is null 
group by j.column_name ,js.job_title,j.category,js.category, lca.job_title) ls
;

select * from h1b_analysis_stg.updated_jobs j
where 
j.column_name =  'Software Engineer'




select column_name, category,count(*) from h1b_analysis_stg.updated_jobs j
group by column_name, category 
having count(*) > 1
;


delete from  h1b_analysis_stg.job_catergory j
where 
ctid in (select ctid from (
select ctid,job_title, category,row_number () over(partition by job_title, category ) row_num
from h1b_analysis_stg.job_catergory j) where row_num > 1) 
;

select * from 
h1b_analysis_stg.job_catergory



select * from  h1b_analysis_stg.stg_lca_disclosure_data lca 
where 
lca.job_title  is null 

