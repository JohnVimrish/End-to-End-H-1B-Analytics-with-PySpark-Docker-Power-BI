 select concat( ' " truncate table ',table_schema ,'.', table_name , ';" ') from information_schema.tables 
 where (table_type = upper('base table') and table_schema  ='h1b_analysis_stg')
 and table_name not like  'test%'
 ;


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
 
 
 
 select case_number, count(*) from h1b_analysis_stg.stg_lca_disclosure_data
 group by case_number
having count(*) > 1
;


select * from h1b_analysis_stg.stg_h1b_data_second
where "YEAR" = 2015 
;


select concat('select ''',table_schema, '.', table_name,''' as table_name,count(*) as table_row_count from ',table_schema, '.', table_name,' union all')
from 
information_schema.tables 
where (table_type = upper('base table') and table_schema  ='h1b_analysis_stg')
and table_name not like  'test%'
;

select case_number,case_status,employer_name,soc_name,soc_code,job_title,"YEAR",full_time_position,count(*) from h1b_analysis_stg.stg_h1b_disclosure_data shbdd 
group by case_number,case_status,employer_name,soc_name,soc_code,job_title,"YEAR",full_time_position
having count(*) > 1;


select *  from h1b_analysis_stg.stg_lca_disclosure_data sldd ;


select case_number,case_Status , count(*)  from h1b_analysis_stg.stg_lca_disclosure_data sldd 
group by case_number,case_status 
having count(*) > 1;

select * from h1b_analysis_stg.stg_lca_disclosure_data sldd 
where 
case_number  = 'I-200-20335-931007'
order by agree_to_lc_statement 

create index ind_basis_for_classification  
on h1b_analysis_stg.stg_basis_for_classification ( basis_of_classification,code_description);


create index ind_ben_education_code 
on h1b_analysis_stg.stg_ben_education_code ( ben_Education_code,code_description);

create index ind_class_preference
on h1b_analysis_stg.stg_class_preference ( class_preference,code_description);

create index ind_country_code
on h1b_analysis_stg.stg_country_code ( country_name);

create unique index uind_h1b_data
on h1b_analysis_stg.stg_h1b_data ( case_number ,case_status);

create  index ind_h1b_data
on h1b_analysis_stg.stg_h1b_data (visa_class,employment_start_date,employment_end_date,employer_name);


create  unique index ind_h1b_data_disclosure_data
on h1b_analysis_stg.stg_h1b_disclosure_data (case_number,case_status,employer_name,soc_name,soc_code,job_title,"YEAR",full_time_position);

create   index ind_job_codes
on h1b_analysis_stg.stg_job_codes (occupation_category,occupation_code,occupation_description);

create   index ind_requested_action
on h1b_analysis_stg.stg_requested_action (requested_action,code_description);



DELETE FROM h1b_analysis_stg.stg_h1b_disclosure_data
WHERE ctid IN
    (SELECT ctid 
    FROM
        (SELECT ctid ,*,ROW_NUMBER() OVER (PARTITION BY case_number,case_status,employer_name,soc_name,soc_code,job_title,full_time_position,prevailing_wage,worksite_city,worksite_state_abb,"YEAR",worksite_state_full,worksite ) AS RN
FROM h1b_analysis_stg.stg_h1b_disclosure_data) t
        WHERE t.RN > 1 );


DELETE FROM h1b_analysis_stg.stg_lca_disclosure_data
WHERE ctid IN
    (SELECT ctid 
    FROM
        (SELECT ctid ,*,ROW_NUMBER() OVER (
        PARTITION BY 
        case_number,case_status,received_date,decision_date,original_cert_date,visa_class,job_title,soc_code,soc_title,
        full_time_position,begin_date,end_date,total_worker_positions,new_employment,continued_employment,change_previous_employment,new_concurrent_employment,change_employer,amended_petition,employer_name,trade_name_dba,employer_address1,employer_address2,employer_city,employer_state,employer_postal_code,employer_country,employer_province,employer_phone,employer_phone_ext,naics_code,employer_poc_last_name,employer_poc_first_name,employer_poc_middle_name,employer_poc_job_title,employer_poc_address1,employer_poc_address2,employer_poc_city,employer_poc_state,employer_poc_postal_code,employer_poc_country,employer_poc_province,employer_poc_phone,employer_poc_phone_ext,employer_poc_email,agent_representing_employer,agent_attorney_last_name,agent_attorney_first_name,agent_attorney_middle_name,agent_attorney_address1,agent_attorney_address2,agent_attorney_city,agent_attorney_state,agent_attorney_postal_code,agent_attorney_country,agent_attorney_province,agent_attorney_phone,agent_attorney_phone_ext,agent_attorney_email_address,lawfirm_name_business_name,state_of_highest_court,name_of_highest_state_court,worksite_workers,secondary_entity,secondary_entity_business_name,worksite_address1,worksite_address2,worksite_city,worksite_county,worksite_state,worksite_postal_code,wage_rate_of_pay_from,wage_rate_of_pay_to,wage_unit_of_pay,prevailing_wage,pw_unit_of_pay,pw_tracking_number,pw_wage_level,pw_oes_year,pw_other_source,pw_other_year,pw_survey_publisher,pw_survey_name,total_worksite_locations,agree_to_lc_statement,h_1b_dependent,willful_violator,support_h1b,statutory_basis,appendix_a_attached,public_disclosure,preparer_last_name,preparer_first_name,preparer_middle_initial,
        preparer_business_name,preparer_email 
        ) AS RN
FROM h1b_analysis_stg.stg_lca_disclosure_data) t
        WHERE t.RN > 1 );
commit ;

select  lottery_year , count(*) from 
(select bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name , fein , zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind 
, receipt_number,rec_date,
wage_amt,wage_unit,valid_from,valid_to,count(*)from h1b_analysis_stg.stg_trk_disclosure_data 
where status_type  ='SELECTED'
--where (employer_name , fein , zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind)
--in 
--(('3 Dots IT Solutions LLC','811992293','27519','Shyamala','Krishnan','2024','ELIGIBLE','1'))
group by   bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name , fein , zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind 
, receipt_number,rec_date,
wage_amt,wage_unit,valid_from,valid_to
having count(*) > 1
order by lottery_year desc) 
ls
group by lottery_year;

select * 
from h1b_analysis_stg.stg_trk_disclosure_data 
where (bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name , fein , zip,agent_first_name,agent_last_name,lottery_year/*,status_type,ben_multi_reg_ind*/ )
in 
(('(b)(6)','IND','IND','(b)(6)','1982','male','3 Dots IT Solutions LLC','811992293','27519','Shyamala','Krishnan','2024'/*,'ELIGIBLE','1'*/),
('(b)(6)','ABW','NLD','(b)(6)','1994','male','American Bridge Company','251607500','15108','William','Eichelberger','2023'))
order by lottery_year:: int desc ;


select  *
from h1b_analysis_stg.stg_trk_disclosure_data 
where (bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name , fein , zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind 
, receipt_number,rec_date,
wage_amt,wage_unit,valid_from,valid_to)
=
(('(b)(6)','BRA','BRA','(b)(6)','1987','male','STRATUS MERIDIAN LLC','830724307','75038','DEEPTI','SURAPANENI','2024','SELECTED','0','(b)(6)','6/22/2023','80850','YEAR','10/1/2023','9/30/2026'))
order by lottery_year:: int desc ;




select  distinct status_type from h1b_analysis_stg.stg_trk_disclosure_data 


delete from h1b_analysis_stg.stg_trk_disclosure_data  
where 
lottery_year  = '(b)(3) (b)(6) (b)(7)(c)'




select
country_of_birth,country_of_nationality,pet_zip,zip, lottery_year,
stdd.worksite_zip,
stdd.ben_year_of_birth ,
stdd.gender,agent_first_name, agent_last_name,fein,lottery_year, status_type, 
ben_multi_reg_ind,receipt_number,rec_date,first_decision,first_decision_date,i129_employer_name,requested_class,
ed_level_definition,ben_pfield_of_study,ben_comp_paid,count(*)
from h1b_analysis_stg.stg_trk_disclosure_data stdd 
group by 
country_of_birth,country_of_nationality,pet_zip,zip, lottery_year,stdd.ben_year_of_birth ,
stdd.worksite_zip,
stdd.gender,agent_first_name, agent_last_name,fein,lottery_year, status_type, ben_multi_reg_ind,receipt_number,rec_date,first_decision,first_decision_date,i129_employer_name,requested_class,
ed_level_definition,ben_pfield_of_study,ben_comp_paid
having count(*) > 1
order by count(*) desc;



select * from h1b_analysis_stg.stg_trk_disclosure_data 
where
lower(employer_name) like  '%' and lower(city) like 'sugarland'
order by  employer_name

select * , 
row_number() over  (partition by 
bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name,
fein,mail_addr,city,state,zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind,
first_decision,i129_employer_name,pet_zip,requested_class,
basis_for_classification,requested_action,number_of_beneficiaries,ben_sex,ben_country_of_birth,ben_current_class,
dol_eta_case_number,worksite_city,worksite_state,worksite_zip,naics_code 
order by  case when lower(coalesce(full_time_ind,'N'))  ='y' then 1 else 0 end desc, receipt_number, rec_date desc ,first_decision_date,case when job_title is null  then  0 else length(job_title) end desc) row_num
from h1b_analysis_stg.stg_trk_disclosure_data stdd 
where (country_of_birth,country_of_nationality,pet_zip,zip,lottery_year,worksite_zip,ben_year_of_birth,gender,agent_first_name,agent_last_name,fein,lottery_year,status_type,ben_multi_reg_ind,receipt_number,rec_date,first_decision,first_decision_date,i129_employer_name,requested_class,ed_level_definition,ben_pfield_of_study,ben_comp_paid) in 
(('CHN','CHN','06902','06902-6834','2021','06902','1991','female','Lauren','Haymond','454015991','2021','SELECTED','0',	'(b)(6)','4/17/2020	Approved','5/21/2020','CASTLETON COMMODITIES MGT SVCS LLC','1B1','MASTER''S DEGREE','COMPUTATIONAL FINANCE','100000'))
order by row_number() over  (partition by 
bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name,
fein,mail_addr,city,state,zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind,
first_decision,i129_employer_name,pet_zip,requested_class,
basis_for_classification,requested_action,number_of_beneficiaries,ben_sex,ben_country_of_birth,ben_current_class,
dol_eta_case_number,worksite_street,worksite_city,worksite_state,worksite_zip,naics_code 
order by  case when lower(coalesce(full_time_ind,'N'))  ='y' then 1 else 0 end desc , receipt_number, rec_date desc ,first_decision_date,case when job_title is null  then  0 else length(job_title) end desc) asc
--(b)(6)	IND	IND	(b)(6)	1996			2024

select ('CHN','CHN','06902','06902-6834','2021','06902','1991','female','Lauren','Haymond','454015991','2021','SELECTED','0',	'(b)(6)','4/17/2020	Approved','5/21/2020','CASTLETON COMMODITIES MGT SVCS LLC','1B1','MASTER''S DEGREE','COMPUTATIONAL FINANCE','100000');

DELETE FROM h1b_analysis_stg.stg_trk_disclosure_data stdd
WHERE ctid IN
    (select ctid
    FROM
        (
        select ctid,* , 
row_number() over  (partition by 
bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name,
fein,mail_addr,city,state,zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind,
first_decision,i129_employer_name,pet_street,pet_city,pet_state,pet_zip,requested_class,
basis_for_classification,requested_action,number_of_beneficiaries,ben_sex,ben_country_of_birth,ben_current_class,
dol_eta_case_number,worksite_street,worksite_city,worksite_state,worksite_zip,full_time_ind,wage_amt,
wage_unit,valid_from,valid_to,num_of_emp_in_us,s1q1a,s1q1b,ben_education_code,ed_level_definition,ben_pfield_of_study,
ben_comp_paid,dot_code,naics_code,s3q1,s4q1,t_u_vawa_flag
order by receipt_number, rec_date desc ,first_decision_date,length(job_title) desc ) row_num
from h1b_analysis_stg.stg_trk_disclosure_data stdd ) t
        WHERE t.row_num > 1 );






DELETE FROM h1b_analysis_stg.stg_trk_disclosure_data stdd
WHERE ctid IN
    (select ctid
    FROM
        (
        select ctid,* , 
       row_number() over(
partition by 
bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name,
fein,mail_addr,city,state,zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind,
first_decision,i129_employer_name,pet_zip,requested_class,
basis_for_classification,requested_action,number_of_beneficiaries,ben_sex,ben_country_of_birth,ben_current_class,
dol_eta_case_number,worksite_city,worksite_state,worksite_zip,naics_code 
order by  case when lower(coalesce(full_time_ind,'N'))  ='y' then 1 else 0 end desc, receipt_number, rec_date desc ,first_decision_date,case when job_title is null  then  0 else length(job_title) end desc ) row_num
from h1b_analysis_stg.stg_trk_disclosure_data stdd ) t
        WHERE t.row_num > 1 );



DELETE FROM h1b_analysis_stg.stg_trk_disclosure_data stdd
WHERE ctid IN
    (select ctid
    FROM
        (
        select ctid,* , 
       row_number() over(
partition by 
bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name,
fein,mail_addr,city,state,zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind,
first_decision,i129_employer_name,pet_zip,requested_class,
basis_for_classification,requested_action,number_of_beneficiaries,ben_sex,ben_country_of_birth,
dol_eta_case_number,worksite_city,worksite_state,worksite_zip,naics_code 
order by case when ben_current_class is null then 0 else 1 end desc,  case when lower(coalesce(full_time_ind,'N'))  ='y' then 1 else 0 end desc, receipt_number, rec_date desc ,first_decision_date,case when job_title is null  then  0 else length(job_title) end desc ) row_num
from h1b_analysis_stg.stg_trk_disclosure_data stdd ) t
        WHERE t.row_num > 1 );



DELETE FROM h1b_analysis_stg.stg_trk_disclosure_data stdd
WHERE ctid IN
    (select ctid
    FROM
        (
        select ctid,* , 
       row_number() over(
partition by 
bcn,country_of_birth,country_of_nationality,ben_date_of_birth,ben_year_of_birth,gender,employer_name,fein,
mail_addr,city,state,zip,agent_first_name,agent_last_name,lottery_year,status_type,ben_multi_reg_ind,receipt_number,
rec_date,first_decision,first_decision_date,i129_employer_name,pet_street,pet_city,pet_state,pet_zip,requested_class,basis_for_classification,
requested_action,number_of_beneficiaries,ben_sex,ben_country_of_birth,ben_current_class,job_title,dol_eta_case_number,worksite_city,
worksite_state,worksite_zip,full_time_ind,wage_amt,wage_unit,valid_from,valid_to,num_of_emp_in_us,
s1q1a,s1q1b,ben_education_code,ed_level_definition,ben_pfield_of_study,ben_comp_paid,dot_code,s3q1,s4q1,t_u_vawa_flag
order by naics_code::int desc   ) row_num
from h1b_analysis_stg.stg_trk_disclosure_data stdd 

where fein in ('133417984')
and zip = '10022'
and agent_first_name  = 'Alberto'
and agent_last_name  = 'Medina'
and status_type  = 'SELECTED'
and rec_date  = '9/26/2023'
and ben_year_of_birth  = '1998') t
        WHERE t.row_num > 1 );

select * from h1b_analysis_stg.stg_trk_disclosure_data
where fein in ('133417984')
and zip = '10022'
and agent_first_name  = 'Alberto'
and agent_last_name  = 'Medina'
and status_type  = 'SELECTED'
and rec_date  = '9/26/2023'
and ben_year_of_birth  = '1998'
