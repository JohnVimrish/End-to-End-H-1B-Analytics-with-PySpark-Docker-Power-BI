select case_Status , count(*) from  h1b_analysis_stg.stg_lca_disclosure_data sldd
group by case_Status;


select * from h1b_analysis_stg.stg_lca_disclosure_data ;

DELETE FROM h1b_analysis_stg.stg_lca_disclosure_data
WHERE ctid IN
    (select ctid
    FROM
        (SELECT ctid ,*,ROW_NUMBER() OVER (
        PARTITION BY 
       case_number,case_status,received_date,decision_date,original_cert_date,visa_class,job_title, CASE 
        WHEN REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(soc_code, '[.-]', '', 'g')
        ELSE soc_code  END ,
      soc_title,full_time_position,begin_date,end_date,total_worker_positions,new_employment,continued_employment,
      change_previous_employment,new_concurrent_employment,change_employer,amended_petition,employer_name,trade_name_dba,
      employer_address1,employer_address2,employer_city,employer_state,
    CASE 
        WHEN employer_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(employer_postal_code, '^0+', '', 'g') 
        ELSE employer_postal_code
    END  ,employer_country,employer_province,
      employer_phone,employer_phone_ext,naics_code,employer_poc_last_name,employer_poc_first_name,employer_poc_middle_name,
      employer_poc_job_title,employer_poc_address1,employer_poc_address2,employer_poc_city,employer_poc_state, CASE 
        WHEN employer_poc_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(employer_poc_postal_code, '^0+', '', 'g') 
        ELSE employer_poc_postal_code
    END,
      employer_poc_country,employer_poc_province,employer_poc_phone,employer_poc_phone_ext,employer_poc_email,agent_representing_employer,
      agent_attorney_last_name,agent_attorney_first_name,agent_attorney_middle_name,agent_attorney_address1,agent_attorney_address2,agent_attorney_city,
      agent_attorney_state,    CASE 
        WHEN agent_attorney_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(agent_attorney_postal_code, '^0+', '', 'g') 
        ELSE agent_attorney_postal_code
    END ,agent_attorney_country,agent_attorney_province,agent_attorney_phone,agent_attorney_phone_ext,
      agent_attorney_email_address,lawfirm_name_business_name,state_of_highest_court,name_of_highest_state_court,worksite_workers,secondary_entity,
      secondary_entity_business_name,worksite_address1,worksite_address2,worksite_city,worksite_county,worksite_state,    CASE 
        WHEN worksite_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(worksite_postal_code, '^0+', '', 'g') 
        ELSE worksite_postal_code
    END ,wage_rate_of_pay_from,
      wage_rate_of_pay_to,wage_unit_of_pay,prevailing_wage,pw_unit_of_pay,pw_tracking_number,pw_wage_level,pw_oes_year,pw_other_source,pw_other_year,
      pw_survey_publisher,pw_survey_name,total_worksite_locations,
         case  WHEN lower(agree_to_lc_statement) in ('y','yes') then 'Y' 
        WHEN lower(agree_to_lc_statement) in ('n','no') then 'N' 
        ELSE agree_to_lc_statement
    END  ,
       CASE 
        WHEN lower(h_1b_dependent) in ('y','yes') then 'Y' 
        WHEN lower(h_1b_dependent) in ('n','no') then 'N' 
        ELSE h_1b_dependent
    END  ,
        CASE 
        WHEN lower(willful_violator) in ('y','yes') then 'Y' 
        WHEN lower(willful_violator) in ('n','no') then 'N' 
        ELSE willful_violator
    END  ,            CASE 
        WHEN lower(support_h1b) in ('y','yes') then 'Y' 
        WHEN lower(support_h1b) in ('n','no') then 'N' 
        ELSE support_h1b
    END ,statutory_basis,
      appendix_a_attached,public_disclosure,preparer_last_name,preparer_first_name,preparer_middle_initial,preparer_business_name,preparer_email ) AS RN
FROM h1b_analysis_stg.stg_lca_disclosure_data) t
        WHERE t.RN > 1 );


DELETE FROM h1b_analysis_stg.stg_lca_disclosure_data
WHERE ctid IN
    (select ctid
    FROM
        (SELECT ctid ,*,ROW_NUMBER() OVER (
        PARTITION BY 
       case_number,case_status,received_date,decision_date,original_cert_date,visa_class,job_title, CASE 
        WHEN REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(soc_code,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE soc_code  end ,
       begin_date,end_date,total_worker_positions,new_employment,continued_employment,
      change_previous_employment,new_concurrent_employment,change_employer,amended_petition,employer_name,trade_name_dba,
      employer_address1,employer_address2,employer_city,employer_state,
    CASE 
        WHEN employer_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(employer_postal_code, '^0+', '', 'g'),'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE employer_postal_code
    END  ,employer_country,employer_province,
      employer_phone,employer_phone_ext,naics_code,employer_poc_last_name,employer_poc_first_name,employer_poc_middle_name,
      employer_poc_job_title,employer_poc_address1,employer_poc_address2,employer_poc_city,employer_poc_state, CASE 
        WHEN employer_poc_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(employer_poc_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE employer_poc_postal_code
    END,
      employer_poc_country,employer_poc_province,employer_poc_phone,employer_poc_phone_ext,employer_poc_email,agent_representing_employer,
      agent_attorney_last_name,agent_attorney_first_name,agent_attorney_middle_name,agent_attorney_address1,agent_attorney_address2,agent_attorney_city,
      agent_attorney_state,      CASE 
        WHEN agent_attorney_postal_code ~ '^[0-9]+$'  or  REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(agent_attorney_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]|"', '', 'g')
        ELSE agent_attorney_postal_code
    end,agent_attorney_country,agent_attorney_province,agent_attorney_phone,agent_attorney_phone_ext,
      agent_attorney_email_address,lawfirm_name_business_name,state_of_highest_court,name_of_highest_state_court,worksite_workers,secondary_entity,
      secondary_entity_business_name, CASE 
        WHEN worksite_address1~ '^[0-9]+$'
        THEN REGEXP_REPLACE(worksite_address1, '^0+', '', 'g')
        ELSE worksite_address1
    end,
       CASE 
        WHEN worksite_address2 ~ '^[0-9]+$'
        THEN REGEXP_REPLACE(worksite_address2, '^0+', '', 'g')
        ELSE worksite_address2
    end,worksite_city,worksite_county,worksite_state,    CASE 
        WHEN worksite_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(worksite_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE worksite_postal_code
    END ,wage_rate_of_pay_from,
      wage_rate_of_pay_to,wage_unit_of_pay,prevailing_wage,pw_unit_of_pay,pw_tracking_number,pw_wage_level,pw_oes_year,pw_other_source,pw_other_year,
      pw_survey_publisher,pw_survey_name,total_worksite_locations,
         case  WHEN lower(agree_to_lc_statement) in ('y','yes') then 'Y' 
        WHEN lower(agree_to_lc_statement) in ('n','no') then 'N' 
        ELSE agree_to_lc_statement
    END  ,
       CASE 
        WHEN lower(h_1b_dependent) in ('y','yes') then 'Y' 
        WHEN lower(h_1b_dependent) in ('n','no') then 'N' 
        ELSE h_1b_dependent
    END  ,
        CASE 
        WHEN lower(willful_violator) in ('y','yes') then 'Y' 
        WHEN lower(willful_violator) in ('n','no') then 'N' 
        ELSE willful_violator
    END  ,            CASE 
        WHEN lower(support_h1b) in ('y','yes') then 'Y' 
        WHEN lower(support_h1b) in ('n','no') then 'N' 
        ELSE support_h1b
    END ,
      public_disclosure,preparer_last_name,preparer_first_name,preparer_middle_initial,preparer_business_name,preparer_email 
      order by  length(appendix_a_attached) desc nulls last ,length(soc_title) desc nulls last ,length(full_time_position) desc nulls last,length(statutory_basis) desc nulls last) AS RN
FROM h1b_analysis_stg.stg_lca_disclosure_data
  ) t
        WHERE t.RN > 1 );

DELETE FROM h1b_analysis_stg.stg_lca_disclosure_data
WHERE ctid IN
    (select ctid
    FROM
        (SELECT ctid ,*,ROW_NUMBER() OVER (
        PARTITION BY 
       case_number,case_status,received_date,decision_date,original_cert_date,visa_class,job_title,
       begin_date,end_date,total_worker_positions,new_employment,continued_employment,
      change_previous_employment,new_concurrent_employment,change_employer,amended_petition,employer_name,trade_name_dba,
      employer_address1,employer_address2,employer_city,employer_state,
    CASE 
        WHEN employer_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(employer_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE employer_postal_code
    END  ,employer_country,employer_province,
      employer_phone,employer_phone_ext,naics_code,employer_poc_last_name,employer_poc_first_name,employer_poc_middle_name,
      employer_poc_job_title,employer_poc_address1,employer_poc_address2,employer_poc_city,employer_poc_state, CASE 
        WHEN employer_poc_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(employer_poc_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE employer_poc_postal_code
    END,
      employer_poc_country,employer_poc_province,employer_poc_phone,employer_poc_phone_ext,employer_poc_email,agent_representing_employer,
      agent_attorney_last_name,agent_attorney_first_name,agent_attorney_middle_name,agent_attorney_address1,agent_attorney_address2,agent_attorney_city,
      agent_attorney_state,    CASE 
        WHEN agent_attorney_postal_code ~ '^[0-9]+$'  or  REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(agent_attorney_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]|"', '', 'g')
        ELSE agent_attorney_postal_code
    end ,agent_attorney_country,agent_attorney_province,agent_attorney_phone,agent_attorney_phone_ext,
      agent_attorney_email_address,lawfirm_name_business_name,state_of_highest_court,name_of_highest_state_court,worksite_workers,secondary_entity,
      secondary_entity_business_name,           CASE 
        WHEN worksite_address1~ '^[0-9]+$'
        THEN REGEXP_REPLACE(worksite_address1, '^0+', '', 'g')
        ELSE worksite_address1
    end,
       CASE 
        WHEN worksite_address2 ~ '^[0-9]+$'
        THEN REGEXP_REPLACE(worksite_address2, '^0+', '', 'g')
        ELSE worksite_address2
    end,worksite_city,worksite_county,worksite_state,    CASE 
        WHEN worksite_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(worksite_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE worksite_postal_code
    END ,wage_rate_of_pay_from,
      wage_rate_of_pay_to,wage_unit_of_pay,prevailing_wage,pw_unit_of_pay,pw_tracking_number,pw_wage_level,pw_oes_year,pw_other_source,pw_other_year,
      pw_survey_publisher,pw_survey_name,total_worksite_locations,
         case  WHEN lower(agree_to_lc_statement) in ('y','yes') then 'Y' 
        WHEN lower(agree_to_lc_statement) in ('n','no') then 'N' 
        ELSE agree_to_lc_statement
    END  ,
       CASE 
        WHEN lower(h_1b_dependent) in ('y','yes') then 'Y' 
        WHEN lower(h_1b_dependent) in ('n','no') then 'N' 
        ELSE h_1b_dependent
    END  ,
        CASE 
        WHEN lower(willful_violator) in ('y','yes') then 'Y' 
        WHEN lower(willful_violator) in ('n','no') then 'N' 
        ELSE willful_violator
    END  ,            CASE 
        WHEN lower(support_h1b) in ('y','yes') then 'Y' 
        WHEN lower(support_h1b) in ('n','no') then 'N' 
        ELSE support_h1b
    END ,
public_disclosure,preparer_last_name,preparer_first_name,preparer_middle_initial,preparer_business_name,preparer_email 
      order by CASE 
        WHEN REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(soc_code,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE soc_code  end  desc, length(appendix_a_attached) desc nulls last ,length(soc_title) desc nulls last ,length(full_time_position) desc nulls last,length(statutory_basis) desc nulls last) AS RN
FROM h1b_analysis_stg.stg_lca_disclosure_data
  ) t
        WHERE t.RN > 1 );


select  case_number ,case_status, count(*) from h1b_analysis_stg.stg_lca_disclosure_data
group by case_number ,case_status
having count(*) > 1 
order by count(*)  desc ;



select * from h1b_analysis_stg.stg_lca_disclosure_data
where case_number in ('I-200-20293-881608','I-200-20316-908325')
order by case_number,decision_date asc

select * from h1b_analysis_stg.stg_lca_disclosure_data sldd 
where case_number  = 'I-200-19302-113431'



SELECT 
    CASE 
        WHEN REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(soc_code, '[.-]', '', 'g')::INT  
        ELSE soc_code::int 
    END AS cleaned_soc_code
FROM h1b_analysis_stg.stg_lca_disclosure_data;

SELECT 
    CASE 
        WHEN employer_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(employer_postal_code, '^0+', '', 'g') 
        ELSE employer_postal_code
    END AS cleaned_postal_code
FROM h1b_analysis_stg.stg_lca_disclosure_data;


select distinct agree_to_lc_statement,h_1b_dependent ,willful_violator FROM h1b_analysis_stg.stg_lca_disclosure_data;

SELECT 
    CASE 
        WHEN lower(agree_to_lc_statement) in ('y','yes') then 'Y' 
        WHEN lower(agree_to_lc_statement) in ('n','no') then 'N' 
        ELSE agree_to_lc_statement
    END AS agree_to_lc_statement,
       CASE 
        WHEN lower(h_1b_dependent) in ('y','yes') then 'Y' 
        WHEN lower(h_1b_dependent) in ('n','no') then 'N' 
        ELSE h_1b_dependent
    END AS h_1b_dependent,
        CASE 
        WHEN lower(willful_violator) in ('y','yes') then 'Y' 
        WHEN lower(willful_violator) in ('n','no') then 'N' 
        ELSE willful_violator
    END AS willful_violator
FROM h1b_analysis_stg.stg_lca_disclosure_data;





select case_number,case_status ,count(*) FROM h1b_analysis_stg.stg_lca_disclosure_data
group by case_number,case_status
having count(*) > 1
order by 3 desc;



SELECT ctid ,*,length(full_time_position) desc ,length(statutory_basis),
CASE 
        WHEN REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(soc_code,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE soc_code  end,
            CASE 
        WHEN worksite_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(worksite_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE worksite_postal_code
    END ,wage_rate_of_pay_from,
      wage_rate_of_pay_to,wage_unit_of_pay,prevailing_wage,pw_unit_of_pay,pw_tracking_number,pw_wage_level,pw_oes_year,pw_other_source,pw_other_year,
      pw_survey_publisher,pw_survey_name,total_worksite_locations,
         case  WHEN lower(agree_to_lc_statement) in ('y','yes') then 'Y' 
        WHEN lower(agree_to_lc_statement) in ('n','no') then 'N' 
        ELSE agree_to_lc_statement
    END  ,
       CASE 
        WHEN lower(h_1b_dependent) in ('y','yes') then 'Y' 
        WHEN lower(h_1b_dependent) in ('n','no') then 'N' 
        ELSE h_1b_dependent
    END  ,
        CASE 
        WHEN lower(willful_violator) in ('y','yes') then 'Y' 
        WHEN lower(willful_violator) in ('n','no') then 'N' 
        ELSE willful_violator
    END  ,            CASE 
        WHEN lower(support_h1b) in ('y','yes') then 'Y' 
        WHEN lower(support_h1b) in ('n','no') then 'N' 
        ELSE support_h1b
    END ,
        CASE 
        WHEN employer_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(employer_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE employer_postal_code
    END ,
     CASE 
        WHEN employer_poc_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(employer_poc_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE employer_poc_postal_code
    end,
    CASE 
        WHEN agent_attorney_postal_code ~ '^[0-9]+$'  or  REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(agent_attorney_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]|"', '', 'g')
        ELSE agent_attorney_postal_code
    end,
      CASE 
        WHEN worksite_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(worksite_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE worksite_postal_code
    end,
ROW_NUMBER() OVER (
        PARTITION BY 
       case_number,case_status,received_date,decision_date,original_cert_date,visa_class,job_title,
       begin_date,end_date,total_worker_positions,new_employment,continued_employment,
      change_previous_employment,new_concurrent_employment,change_employer,amended_petition,employer_name,trade_name_dba,
      employer_address1,employer_address2,employer_city,employer_state,
    CASE 
        WHEN employer_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(employer_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE employer_postal_code
    END  ,employer_country,employer_province,
      employer_phone,employer_phone_ext,naics_code,employer_poc_last_name,employer_poc_first_name,employer_poc_middle_name,
      employer_poc_job_title,employer_poc_address1,employer_poc_address2,employer_poc_city,employer_poc_state, CASE 
        WHEN employer_poc_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(employer_poc_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE employer_poc_postal_code
    END,
      employer_poc_country,employer_poc_province,employer_poc_phone,employer_poc_phone_ext,employer_poc_email,agent_representing_employer,
      agent_attorney_last_name,agent_attorney_first_name,agent_attorney_middle_name,agent_attorney_address1,agent_attorney_address2,agent_attorney_city,
      agent_attorney_state,     CASE 
        WHEN agent_attorney_postal_code ~ '^[0-9]+$'  or  REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(agent_attorney_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]|"', '', 'g')
        ELSE agent_attorney_postal_code
    end ,
      CASE 
        WHEN worksite_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(worksite_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE worksite_postal_code
    end,agent_attorney_country,agent_attorney_province,agent_attorney_phone,agent_attorney_phone_ext,
      agent_attorney_email_address,lawfirm_name_business_name,state_of_highest_court,name_of_highest_state_court,worksite_workers,secondary_entity,
      secondary_entity_business_name,
         CASE 
        WHEN agent_attorney_postal_code ~ '^[0-9]+$'  or  REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(agent_attorney_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]|"', '', 'g')
        ELSE agent_attorney_postal_code
    end,
           CASE 
        WHEN worksite_address1~ '^[0-9]+$'
        THEN REGEXP_REPLACE(worksite_address1, '^0+', '', 'g')
        ELSE worksite_address1
    end,
       CASE 
        WHEN worksite_address2 ~ '^[0-9]+$'
        THEN REGEXP_REPLACE(worksite_address2, '^0+', '', 'g')
        ELSE worksite_address2
    end
    ,worksite_city,worksite_county,worksite_state,    CASE 
        WHEN worksite_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(worksite_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE worksite_postal_code
    END ,wage_rate_of_pay_from,
      wage_rate_of_pay_to,wage_unit_of_pay,prevailing_wage,pw_unit_of_pay,pw_tracking_number,pw_wage_level,pw_oes_year,pw_other_source,pw_other_year,
      pw_survey_publisher,pw_survey_name,total_worksite_locations,
         case  WHEN lower(agree_to_lc_statement) in ('y','yes') then 'Y' 
        WHEN lower(agree_to_lc_statement) in ('n','no') then 'N' 
        ELSE agree_to_lc_statement
    END  ,
       CASE 
        WHEN lower(h_1b_dependent) in ('y','yes') then 'Y' 
        WHEN lower(h_1b_dependent) in ('n','no') then 'N' 
        ELSE h_1b_dependent
    END  ,
        CASE 
        WHEN lower(willful_violator) in ('y','yes') then 'Y' 
        WHEN lower(willful_violator) in ('n','no') then 'N' 
        ELSE willful_violator
    END  ,            CASE 
        WHEN lower(support_h1b) in ('y','yes') then 'Y' 
        WHEN lower(support_h1b) in ('n','no') then 'N' 
        ELSE support_h1b
    END ,public_disclosure,preparer_last_name,preparer_first_name,preparer_middle_initial,preparer_business_name,preparer_email 
      order by CASE 
        WHEN REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(soc_code,'\.[0]+$', '', 'g'), '[.-]', '', 'g')
        ELSE soc_code  end  desc, length(appendix_a_attached) desc null last ,length(soc_title) desc nulls last ,length(full_time_position) desc nulls last,length(statutory_basis) desc nulls last) AS RN
FROM h1b_analysis_stg.stg_lca_disclosure_data
where case_number  in ('I-200-23303-464938',
'I-200-23286-431022',
'I-200-20342-940480',
'I-200-23271-388903',
'I-200-23269-381471',
'I-200-20303-891936',
'I-200-23271-390422',
'I-200-23269-381803',
'I-200-20281-869698',
'I-200-23290-438189',
'I-200-23269-381475')
order by case_number ;


SELECT 
    CASE 
        WHEN worksite_postal_code ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(worksite_postal_code, '^0+', '', 'g') 
        ELSE worksite_postal_code
    END AS employer_poc_postal_code
FROM h1b_analysis_stg.stg_lca_disclosure_data;

worksite_address1,
worksite_address2

select case_number , * ,
    CASE 
        WHEN agent_attorney_postal_code ~ '^[0-9]+$'  or  REGEXP_REPLACE(soc_code, '[.-]', '', 'g') ~ '^[0-9]+$' 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(agent_attorney_postal_code, '^0+', '', 'g') ,'\.[0]+$', '', 'g'), '[.-]|"', '', 'g')
        ELSE agent_attorney_postal_code
    end,
    agent_attorney_postal_code
    FROM h1b_analysis_stg.stg_lca_disclosure_data
where case_number  in ('I-200-20342-940480','I-200-20281-869698')
order by case_number ;


create unique index uind_lca_disclosure 
on h1b_analysis_stg.stg_lca_disclosure_data(case_number,case_status)
;
