/***************************************************************************************************************/
/* Purpose:  (1) Typecast transform & Generate Simple, Balanced, Random Samples for each year for ingestion    */
/*               + (2) Union all the samples for the interim dataset for additional wrangling with pandas      */                                                                                           */
/*                                                                                                             */
/* Author: Blake Zenuni, Summer 2019                                                                           */
/* Date Created:  Aug. 01, 2019                                                                                */
/* Last Modified: Sep. 03, 2019                                                                                */
/*                                                                                                             */
/***************************************************************************************************************/


/*---------------------------------------------------------------------------------------------------------*/
      --> NB: Latin abbreviation for NOTA BENE, meaning "note well" <--
/*---------------------------------------------------------------------------------------------------------*/


 -- NB1: In this SQL script, simple random samples are balanced for outcomes (50/50 loans approved vs. loans denied).
 -- NB2: Script 03a applies this same logic, but for unbalanced outcomes.






/*======================== 03b. Balanced Outcomes - Simple random samples for HMDA 2010-2017 =========================*/





-- Creating schema and setting users/role for accessibility profiles
CREATE EXTENSION dblink;
CREATE SCHEMA interim_datasets ;
CREATE ROLE reporting_user WITH LOGIN PASSWORD 'team_loan_canoe2019' ;
GRANT USAGE ON SCHEMA interim_datasets TO reporting_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA interim_datasets TO reporting_user ;
--



--> z_bz_AWS_paddleloancanoe <---

/*---------------------------------------------------- HMDA 2010 -----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets.hmda_lar_ii_2010_randsimpl_bal25k ;
--
WITH

     hmda_2010_transform As
     ( SELECT
              --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm10.action_taken_name = 'Loan originated' THEN 1 ELSE 0 END As action_taken,
           CAST(hm10.respondent_id As VARCHAR(28)) As respondent_id, hm10.as_of_year As action_year,
              --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm10.tract_to_msamd_income IS NULL THEN NULL ELSE hm10.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC --NB: must be numeric bc numeric stores decimal places, INT is whole numbers only
               )
           As tract_to_msamd_inc,
           hm10.population As pop,
           ROUND(hm10.minority_population, 2) As minority_pop_perc,
           hm10.number_of_owner_occupied_units As num_owoc_units,
           hm10.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm10.loan_amount_000s As ln_amt_000s,
           hm10.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm10.applicant_income_000s IS NULL THEN NULL ELSE hm10.applicant_income_000s END
                          As Varchar(5)
                     ) As INT
               ) As applic_inc_000s,
           CAST(hm10.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm10.property_type_name As VARCHAR(128) ) As property_type_nm,
           CAST(hm10.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm10.msamd_name As VARCHAR(128)) As msamd_nm,
           CAST(hm10.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm10.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm10.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm10.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm10.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm10.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm10.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm10.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm10.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm10.agency_abbr As VARCHAR(28)) As agency_abbr

       FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2010_allrecords hm10

       --NB: we drop the tuples with loan actions that do not align with our balanced random sample of outcome =1 or 0
       WHERE hm10.action_taken_name In ( 'Loan originated', 'Application denied by financial institution')

       ORDER BY random()
       LIMIT 100000
     ) ,

    hmda_2010_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2010_transform WHERE action_taken = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2010_transform WHERE action_taken = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm10_u.*
  INTO interim_datasets.hmda_lar_ii_2010_randsimpl_bal25k
  FROM hmda_2010_union hm10_u
  --NB: we explicitly specify we want results generated for non-missing values in our data set
  WHERE (
          hm10_u.respondent_id Is Not Null          AND     hm10_u.action_year Is Not Null         AND
          hm10_u.tract_to_msamd_inc Is Not Null     AND     hm10_u.pop Is Not Null                 AND
          hm10_u.minority_pop_perc Is Not Null      AND     hm10_u.num_owoc_units Is Not Null      AND
          hm10_u.num_1to4_fam_units Is Not Null     AND     hm10_u.hud_med_fm_inc Is Not Null      AND
          hm10_u.ln_amt_000s Is Not Null            AND     hm10_u.applic_inc_000s Is Not Null     AND
          hm10_u.state_abbr Is Not Null             AND     hm10_u.property_type_nm Is Not Null    AND
          hm10_u.own_occ_nm Is Not Null             AND     hm10_u.msamd_nm Is Not Null            AND
          hm10_u.ln_type_nm Is Not Null             AND     hm10_u.lien_status_nm Is Not Null      AND
          hm10_u.hoep_status_nm Is Not Null         AND     hm10_u.co_appl_sex Is Not Null         AND
          hm10_u.co_appl_race Is Not Null           AND     hm10_u.co_appl_ethn Is Not Null        AND
          hm10_u.applic_sex Is Not Null             AND     hm10_u.applic_race Is Not Null         AND
          hm10_u.applic_ethn Is Not Null            AND     hm10_u.agency_abbr Is Not Null
        )
ORDER BY random()
LIMIT 25000
;
/*--------------------------- end HMDA 2010 ---------------------------*/

--> END z_bz_AWS_paddleloancanoe <---





/* ==> NB: All subsequent HMDA individual years will apply the same SQL logic from the code above, but will be stripped
       of comments for length
*/





---> z_tn_AWS_paddleloancanoe <---

/*---------------------------------------------------- HMDA 2011 -----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets.hmda_lar_ii_2011_randsimpl_bal25k ;
--
WITH hmda_2011_transform As
 ( SELECT CASE WHEN hm11.action_taken_name = 'Loan originated' THEN 1 ELSE 0 END As action_taken,
    CAST(hm11.respondent_id As VARCHAR(28)) As respondent_id, hm11.as_of_year As action_year,
    CAST( CAST( CASE WHEN hm11.tract_to_msamd_income IS NULL THEN NULL ELSE hm11.tract_to_msamd_income END
    As Varchar(5)) As NUMERIC) As tract_to_msamd_inc, hm11.population As pop,
    ROUND(hm11.minority_population, 2) As minority_pop_perc, hm11.number_of_owner_occupied_units As num_owoc_units,
    hm11.number_of_1_to_4_family_units As num_1to4_fam_units, hm11.loan_amount_000s As ln_amt_000s,
    hm11.hud_median_family_income As hud_med_fm_inc,
    CAST( CAST( CASE WHEN hm11.applicant_income_000s IS NULL THEN NULL ELSE hm11.applicant_income_000s END
    As Varchar(5)) As INT) As applic_inc_000s, CAST(hm11.state_abbr As VARCHAR(5)) As state_abbr,
    CAST(hm11.property_type_name As VARCHAR(128) ) As property_type_nm,
    CAST(hm11.owner_occupancy_name As VARCHAR(128)) As own_occ_nm, CAST(hm11.msamd_name As VARCHAR(128)) As msamd_nm,
    CAST(hm11.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm11.lien_status_name As VARCHAR(56)) As lien_status_nm,
    CAST(hm11.hoepa_status_name As VARCHAR(56)) As hoep_status_nm, CAST(hm11.agency_abbr As VARCHAR(28)) As agency_abbr,
    CAST(hm11.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
    CAST(hm11.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
    CAST(hm11.applicant_sex_name As VARCHAR(28)) As applic_sex,
    CAST(hm11.applicant_race_name_1 As VARCHAR(28)) As applic_race,
    CAST(hm11.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
    CAST(hm11.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex
   FROM public.hmda_lar_2011_allrecords hm11
   WHERE hm11.action_taken_name In ( 'Loan originated', 'Application denied by financial institution')
   ORDER BY random() LIMIT 100000
) ,
 hmda_2011_union AS
 ( SELECT hm_a.* FROM(SELECT * FROM hmda_2011_transform WHERE action_taken = 1 ORDER BY random() LIMIT 12500) hm_a
         UNION ALL
   SELECT hm_a.* FROM(SELECT * FROM hmda_2011_transform WHERE action_taken = 0 ORDER BY random() LIMIT 12500) hm_a
)
SELECT hm11_u.*
  INTO interim_datasets.hmda_lar_ii_2011_randsimpl_bal25k
  FROM hmda_2011_union hm11_u
  WHERE ( hm11_u.respondent_id Is Not Null AND hm11_u.action_year Is Not Null AND hm11_u.tract_to_msamd_inc Is Not Null
          AND hm11_u.pop Is Not Null AND hm11_u.minority_pop_perc Is Not Null AND hm11_u.num_owoc_units Is Not Null
          AND hm11_u.num_1to4_fam_units Is Not Null AND hm11_u.hud_med_fm_inc Is Not Null
          AND hm11_u.ln_amt_000s Is Not Null AND hm11_u.applic_inc_000s Is Not Null AND hm11_u.state_abbr Is Not Null
          AND hm11_u.property_type_nm Is Not Null AND hm11_u.own_occ_nm Is Not Null AND hm11_u.msamd_nm Is Not Null
          AND hm11_u.ln_type_nm Is Not Null AND hm11_u.lien_status_nm Is Not Null AND hm11_u.hoep_status_nm Is Not Null
          AND hm11_u.co_appl_sex Is Not Null AND hm11_u.co_appl_race Is Not Null AND hm11_u.co_appl_ethn Is Not Null
          AND hm11_u.applic_sex Is Not Null AND hm11_u.applic_race Is Not Null AND hm11_u.applic_ethn Is Not Null
          AND hm11_u.agency_abbr Is Not Null
        )
ORDER BY random() LIMIT 12500
;
/*--------------------------- end HMDA 2011 ---------------------------*/





/*--------------------------------------- HMDA 2012 -----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets.hmda_lar_ii_2012_randsimpl_bal25k ;
--
WITH hmda_2012_transform As
 ( SELECT CASE WHEN hm12.action_taken_name = 'Loan originated' THEN 1 ELSE 0 END As action_taken,
    CAST(hm12.respondent_id As VARCHAR(28)) As respondent_id, hm12.as_of_year As action_year,
    CAST( CAST( CASE WHEN hm12.tract_to_msamd_income IS NULL THEN NULL ELSE hm12.tract_to_msamd_income END
    As Varchar(5)) As NUMERIC) As tract_to_msamd_inc, hm12.population As pop,
    ROUND(hm12.minority_population, 2) As minority_pop_perc, hm12.number_of_owner_occupied_units As num_owoc_units,
    hm12.number_of_1_to_4_family_units As num_1to4_fam_units, hm12.loan_amount_000s As ln_amt_000s,
    hm12.hud_median_family_income As hud_med_fm_inc,
    CAST( CAST( CASE WHEN hm12.applicant_income_000s IS NULL THEN NULL ELSE hm12.applicant_income_000s END
    As Varchar(5)) As INT) As applic_inc_000s, CAST(hm12.state_abbr As VARCHAR(5)) As state_abbr,
    CAST(hm12.property_type_name As VARCHAR(128) ) As property_type_nm,
    CAST(hm12.owner_occupancy_name As VARCHAR(128)) As own_occ_nm, CAST(hm12.msamd_name As VARCHAR(128)) As msamd_nm,
    CAST(hm12.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm12.lien_status_name As VARCHAR(56)) As lien_status_nm,
    CAST(hm12.hoepa_status_name As VARCHAR(56)) As hoep_status_nm, CAST(hm12.agency_abbr As VARCHAR(28)) As agency_abbr,
    CAST(hm12.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
    CAST(hm12.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
    CAST(hm12.applicant_sex_name As VARCHAR(28)) As applic_sex,
    CAST(hm12.applicant_race_name_1 As VARCHAR(28)) As applic_race,
    CAST(hm12.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
    CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex
   FROM public.hmda_lar_2012_allrecords hm12
   WHERE hm12.action_taken_name In ( 'Loan originated', 'Application denied by financial institution')
   ORDER BY random() LIMIT 100000
) ,
 hmda_2012_union AS
 ( SELECT hm_a.* FROM(SELECT * FROM hmda_2012_transform WHERE action_taken = 1 ORDER BY random() LIMIT 12500) hm_a
         UNION ALL
   SELECT hm_a.* FROM(SELECT * FROM hmda_2012_transform WHERE action_taken = 0 ORDER BY random() LIMIT 12500) hm_a
)
SELECT hm12_u.*
  INTO interim_datasets.hmda_lar_ii_2012_randsimpl_bal25k
  FROM hmda_2012_union hm12_u
  WHERE ( hm12_u.respondent_id Is Not Null AND hm12_u.action_year Is Not Null AND hm12_u.tract_to_msamd_inc Is Not Null
          AND hm12_u.pop Is Not Null AND hm12_u.minority_pop_perc Is Not Null AND hm12_u.num_owoc_units Is Not Null
          AND hm12_u.num_1to4_fam_units Is Not Null AND hm12_u.hud_med_fm_inc Is Not Null
          AND hm12_u.ln_amt_000s Is Not Null AND hm12_u.applic_inc_000s Is Not Null AND hm12_u.state_abbr Is Not Null
          AND hm12_u.property_type_nm Is Not Null AND hm12_u.own_occ_nm Is Not Null AND hm12_u.msamd_nm Is Not Null
          AND hm12_u.ln_type_nm Is Not Null AND hm12_u.lien_status_nm Is Not Null AND hm12_u.hoep_status_nm Is Not Null
          AND hm12_u.co_appl_sex Is Not Null AND hm12_u.co_appl_race Is Not Null AND hm12_u.co_appl_ethn Is Not Null
          AND hm12_u.applic_sex Is Not Null AND hm12_u.applic_race Is Not Null AND hm12_u.applic_ethn Is Not Null
          AND hm12_u.agency_abbr Is Not Null
        )
ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2012 ---------------------------*/





/*--------------------------------------- HMDA 2013 -----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets.hmda_lar_ii_2013_randsimpl_bal25k ;
--
WITH hmda_2013_transform As
 ( SELECT CASE WHEN hm13.action_taken_name = 'Loan originated' THEN 1 ELSE 0 END As action_taken,
    CAST(hm13.respondent_id As VARCHAR(28)) As respondent_id, hm13.as_of_year As action_year,
    CAST( CAST( CASE WHEN hm13.tract_to_msamd_income IS NULL THEN NULL ELSE hm13.tract_to_msamd_income END
    As Varchar(5)) As NUMERIC) As tract_to_msamd_inc, hm13.population As pop,
    ROUND(hm13.minority_population, 2) As minority_pop_perc, hm13.number_of_owner_occupied_units As num_owoc_units,
    hm13.number_of_1_to_4_family_units As num_1to4_fam_units, hm13.loan_amount_000s As ln_amt_000s,
    hm13.hud_median_family_income As hud_med_fm_inc,
    CAST( CAST( CASE WHEN hm13.applicant_income_000s IS NULL THEN NULL ELSE hm13.applicant_income_000s END
    As Varchar(5)) As INT) As applic_inc_000s, CAST(hm13.state_abbr As VARCHAR(5)) As state_abbr,
    CAST(hm13.property_type_name As VARCHAR(128) ) As property_type_nm,
    CAST(hm13.owner_occupancy_name As VARCHAR(128)) As own_occ_nm, CAST(hm13.msamd_name As VARCHAR(128)) As msamd_nm,
    CAST(hm13.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm13.lien_status_name As VARCHAR(56)) As lien_status_nm,
    CAST(hm13.hoepa_status_name As VARCHAR(56)) As hoep_status_nm, CAST(hm13.agency_abbr As VARCHAR(28)) As agency_abbr,
    CAST(hm13.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
    CAST(hm13.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
    CAST(hm13.applicant_sex_name As VARCHAR(28)) As applic_sex,
    CAST(hm13.applicant_race_name_1 As VARCHAR(28)) As applic_race,
    CAST(hm13.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
    CAST(hm13.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex
   FROM public.hmda_lar_2013_allrecords hm13
   WHERE hm13.action_taken_name In ( 'Loan originated', 'Application denied by financial institution')
   ORDER BY random() LIMIT 100000
) ,
 hmda_2013_union AS
 ( SELECT hm_a.* FROM(SELECT * FROM hmda_2013_transform WHERE action_taken = 1 ORDER BY random() LIMIT 12500) hm_a
         UNION ALL
   SELECT hm_a.* FROM(SELECT * FROM hmda_2013_transform WHERE action_taken = 0 ORDER BY random() LIMIT 12500) hm_a
)
SELECT hm13_u.*
  INTO interim_datasets.hmda_lar_ii_2013_randsimpl_bal25k
  FROM hmda_2013_union hm13_u
  WHERE ( hm13_u.respondent_id Is Not Null AND hm13_u.action_year Is Not Null AND hm13_u.tract_to_msamd_inc Is Not Null
          AND hm13_u.pop Is Not Null AND hm13_u.minority_pop_perc Is Not Null AND hm13_u.num_owoc_units Is Not Null
          AND hm13_u.num_1to4_fam_units Is Not Null AND hm13_u.hud_med_fm_inc Is Not Null
          AND hm13_u.ln_amt_000s Is Not Null AND hm13_u.applic_inc_000s Is Not Null AND hm13_u.state_abbr Is Not Null
          AND hm13_u.property_type_nm Is Not Null AND hm13_u.own_occ_nm Is Not Null AND hm13_u.msamd_nm Is Not Null
          AND hm13_u.ln_type_nm Is Not Null AND hm13_u.lien_status_nm Is Not Null AND hm13_u.hoep_status_nm Is Not Null
          AND hm13_u.co_appl_sex Is Not Null AND hm13_u.co_appl_race Is Not Null AND hm13_u.co_appl_ethn Is Not Null
          AND hm13_u.applic_sex Is Not Null AND hm13_u.applic_race Is Not Null AND hm13_u.applic_ethn Is Not Null
          AND hm13_u.agency_abbr Is Not Null
        )
ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2013 ---------------------------*/




      /*---------------------------------------------------------------------------------------------*/

      /*-------------------------------------- Union 2011-2013 --------------------------------------*/
            DROP TABLE IF EXISTS interim_datasets.hmda_lar_union_ii_2011_to_2013_simplerand_bal75k;
                ;
                WITH
                   hmda_union_2011_2013 AS
                   (
                     SELECT hm11.* FROM interim_datasets.hmda_lar_ii_2011_randsimpl_bal25k hm11
                        UNION ALL
                     SELECT hm12.* FROM interim_datasets.hmda_lar_ii_2012_randsimpl_bal25k hm12
                        UNION ALL
                     SELECT hm13.* FROM interim_datasets.hmda_lar_ii_2013_randsimpl_bal25k hm13
                   )
                SELECT hm_u.*
                  INTO interim_datasets.hmda_lar_union_ii_2011_to_2013_simplerand_bal75k
                  FROM hmda_union_2011_2013 hm_u
                ;
      /*---------------------------------------------------------------------------------------------*/


--> END z_tn_AWS_paddleloancanoe <---





--> z_bz_AWS_paddleloancanoe <---
    --
CREATE EXTENSION dblink;
CREATE SCHEMA raw_datasets ;
CREATE ROLE reporting_user WITH LOGIN PASSWORD 'team_loan_canoe2019' ;
GRANT USAGE ON SCHEMA raw_datasets TO reporting_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_datasets TO reporting_user ;
    --

/*----------------------------------------------------- HMDA 2014 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_ii_2014_randsimpl_bal25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_ii_2014_randsimpl_bal25k
;
--
WITH

     hmda_2014_approved As
     ( SELECT
           1 As action_taken, hm14.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm14.tract_to_msamd_income IS NULL THEN NULL ELSE hm14.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm14.population, ROUND(hm14.minority_population, 2) As min_pop_perc,
           hm14.number_of_owner_occupied_units As num_owoc_units,
           hm14.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm14.loan_amount_000s As ln_amt_000s, hm14.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm14.applicant_income_000s = '' THEN NULL ELSE hm14.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm14.owner_occupancy_name As VARCHAR(148)) As own_occ_nm,
           CAST(hm14.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm14.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm14.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm14.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm14.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm14.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm14.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm14.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm14.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm14.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM paddle_loan_canoe.usa_mortgage_market .hmda_lar_2014_allrecords hm14
       WHERE hm14.action_taken_name
                 In ( 'Application approved but not accepted','Loan originated', 'Loan purchased by the institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2014_denied As
     ( SELECT
           0 As action_taken, hm14.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm14.tract_to_msamd_income IS NULL THEN NULL ELSE hm14.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm14.population, ROUND(hm14.minority_population, 2) As min_pop_perc,
           hm14.number_of_owner_occupied_units As num_owoc_units,
           hm14.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm14.loan_amount_000s As ln_amt_000s, hm14.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm14.applicant_income_000s = '' THEN NULL ELSE hm14.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm14.owner_occupancy_name As VARCHAR(148)) As own_occ_nm,
           CAST(hm14.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm14.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm14.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm14.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm14.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm14.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm14.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm14.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm14.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm14.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2014_allrecords hm14
       WHERE hm14.action_taken_name  In ('Application denied by financial institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2014_balanced AS
     ( SELECT hm14_app.* From hmda_2014_approved hm14_app
            UNION ALL
       SELECT hm14_den.* FROM hmda_2014_denied hm14_den
     )

SELECT hm14_bal.*
INTO interim_datasets.hmda_lar_ii_2014_randsimpl_bal25k
FROM hmda_2014_balanced hm14_bal
;
/*--------------------------- end HMDA 2014 ---------------------------*/



/*---------------------------------------------------- HMDA 2015 -----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets.hmda_lar_ii_2015_randsimpl_bal25k ;
--
WITH

     hmda_2015_transform As
     ( SELECT
              --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm15.action_taken_name = 'Loan originated' THEN 1 ELSE 0 END As action_taken,
           CAST(hm15.respondent_id As VARCHAR(28)) As respondent_id, hm15.as_of_year As action_year,
              --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm15.tract_to_msamd_income IS NULL THEN NULL ELSE hm15.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC --NB: must be numeric bc numeric stores decimal places, INT is whole numbers only
               )
           As tract_to_msamd_inc,
           hm15.population As pop,
           ROUND(hm15.minority_population, 2) As minority_pop_perc,
           hm15.number_of_owner_occupied_units As num_owoc_units,
           hm15.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm15.loan_amount_000s As ln_amt_000s,
           hm15.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm15.applicant_income_000s IS NULL THEN NULL ELSE hm15.applicant_income_000s END
                          As Varchar(5)
                     ) As INT
               ) As applic_inc_000s,
           CAST(hm15.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm15.property_type_name As VARCHAR(128) ) As property_type_nm,
           CAST(hm15.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm15.msamd_name As VARCHAR(128)) As msamd_nm,
           CAST(hm15.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm15.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm15.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm15.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm15.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm15.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm15.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm15.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm15.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm15.agency_abbr As VARCHAR(28)) As agency_abbr

       FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2015_allrecords hm15

       --NB: we drop the tuples with loan actions that do not align with our balanced random sample of outcome =1 or 0
       WHERE hm15.action_taken_name In ( 'Loan originated', 'Application denied by financial institution')

       ORDER BY random()
       LIMIT 100000
     ) ,

    hmda_2015_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2015_transform WHERE action_taken = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2015_transform WHERE action_taken = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm15_u.*
  INTO interim_datasets.hmda_lar_ii_2015_randsimpl_bal25k
  FROM hmda_2015_union hm15_u
  WHERE ( hm15_u.respondent_id Is Not Null AND hm15_u.action_year Is Not Null AND hm15_u.tract_to_msamd_inc Is Not Null
          AND hm15_u.pop Is Not Null AND hm15_u.minority_pop_perc Is Not Null AND hm15_u.num_owoc_units Is Not Null
          AND hm15_u.num_1to4_fam_units Is Not Null AND hm15_u.hud_med_fm_inc Is Not Null
          AND hm15_u.ln_amt_000s Is Not Null AND hm15_u.applic_inc_000s Is Not Null AND hm15_u.state_abbr Is Not Null
          AND hm15_u.property_type_nm Is Not Null AND hm15_u.own_occ_nm Is Not Null AND hm15_u.msamd_nm Is Not Null
          AND hm15_u.ln_type_nm Is Not Null AND hm15_u.lien_status_nm Is Not Null AND hm15_u.hoep_status_nm Is Not Null
          AND hm15_u.co_appl_sex Is Not Null AND hm15_u.co_appl_race Is Not Null AND hm15_u.co_appl_ethn Is Not Null
          AND hm15_u.applic_sex Is Not Null AND hm15_u.applic_race Is Not Null AND hm15_u.applic_ethn Is Not Null
          AND hm15_u.agency_abbr Is Not Null
        )
ORDER BY random()
LIMIT 12500
;





        /*---------------------------------- Union 2014-2015 ----------------------------------*/
                ;
                WITH
                   hmda_union_2014_2015 AS
                   (
                     SELECT hm14.* FROM interim_datasets.hmda_lar_ii_2014_randsimpl_bal25k hm14
                        UNION ALL
                     SELECT hm15.* FROM interim_datasets.hmda_lar_ii_2015_randsimpl_bal25k hm15
                   )
                SELECT hm_u.*
                  INTO interim_datasets.hmda_lar_union_ii_2014_to_2015_simplerand_bal50k
                  FROM hmda_union_2014_2015 hm_u
                ;
        /*-------------------------------------------------------------------------------------*/


--> END z_bz_AWS_paddleloancanoe <---





--> z_ak_AWS_paddleloancanoe <---

/*----------------------------------------------------- HMDA 2016 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_ii_2016_randsimpl_bal25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_ii_2016_randsimpl_bal25k
;
--
WITH

     hmda_2016_approved As
     ( SELECT
           1 As action_taken, hm16.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm16.tract_to_msamd_income IS NULL THEN NULL ELSE hm16.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm16.population, ROUND(hm16.minority_population, 2) As min_pop_perc,
           hm16.number_of_owner_occupied_units As num_owoc_units,
           hm16.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm16.loan_amount_000s As ln_amt_000s, hm16.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm16.applicant_income_000s = '' THEN NULL ELSE hm16.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm16.owner_occupancy_name As VARCHAR(168)) As own_occ_nm,
           CAST(hm16.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm16.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm16.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm16.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm16.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm16.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm16.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm16.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm16.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm16.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM paddle_loan_canoe.usa_mortgage_market .hmda_lar_2016_allrecords hm16
       WHERE hm16.action_taken_name
                 In ( 'Application approved but not accepted','Loan originated', 'Loan purchased by the institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2016_denied As
     ( SELECT
           0 As action_taken, hm16.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm16.tract_to_msamd_income IS NULL THEN NULL ELSE hm16.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm16.population, ROUND(hm16.minority_population, 2) As min_pop_perc,
           hm16.number_of_owner_occupied_units As num_owoc_units,
           hm16.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm16.loan_amount_000s As ln_amt_000s, hm16.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm16.applicant_income_000s = '' THEN NULL ELSE hm16.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm16.owner_occupancy_name As VARCHAR(168)) As own_occ_nm,
           CAST(hm16.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm16.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm16.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm16.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm16.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm16.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm16.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm16.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm16.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm16.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2016_allrecords hm16
       WHERE hm16.action_taken_name  In ('Application denied by financial institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2016_balanced AS
     ( SELECT hm16_app.* From hmda_2016_approved hm16_app
            UNION ALL
       SELECT hm16_den.* FROM hmda_2016_denied hm16_den
     )

SELECT hm16_bal.*
INTO interim_datasets.hmda_lar_ii_2016_randsimpl_bal25k
FROM hmda_2016_balanced hm16_bal
;
/*--------------------------- end HMDA 2016 ---------------------------*/



/*----------------------------------------------------- HMDA 2017 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_ii_2017_randsimpl_bal25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_ii_2017_randsimpl_bal25k
;
--
WITH

     hmda_2017_approved As
     ( SELECT
           1 As action_taken, hm17.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm17.tract_to_msamd_income IS NULL THEN NULL ELSE hm17.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm17.population, ROUND(hm17.minority_population, 2) As min_pop_perc,
           hm17.number_of_owner_occupied_units As num_owoc_units,
           hm17.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm17.loan_amount_000s As ln_amt_000s, hm17.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm17.applicant_income_000s = '' THEN NULL ELSE hm17.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm17.owner_occupancy_name As VARCHAR(178)) As own_occ_nm,
           CAST(hm17.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm17.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm17.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm17.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm17.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm17.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm17.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm17.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm17.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm17.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2017_allrecords hm17
       WHERE hm17.action_taken_name
                 In ( 'Application approved but not accepted','Loan originated', 'Loan purchased by the institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2017_denied As
     ( SELECT
           0 As action_taken, hm17.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm17.tract_to_msamd_income IS NULL THEN NULL ELSE hm17.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm17.population, ROUND(hm17.minority_population, 2) As min_pop_perc,
           hm17.number_of_owner_occupied_units As num_owoc_units,
           hm17.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm17.loan_amount_000s As ln_amt_000s, hm17.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm17.applicant_income_000s = '' THEN NULL ELSE hm17.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm17.owner_occupancy_name As VARCHAR(178)) As own_occ_nm,
           CAST(hm17.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm17.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm17.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm17.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm17.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm17.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm17.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm17.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm17.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm17.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2017_allrecords hm17
       WHERE hm17.action_taken_name  In ('Application denied by financial institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2017_balanced AS
     ( SELECT hm17_app.* From hmda_2017_approved hm17_app
            UNION ALL
       SELECT hm17_den.* FROM hmda_2017_denied hm17_den
     )

SELECT hm17_bal.*
INTO interim_datasets.hmda_lar_ii_2017_randsimpl_bal25k
FROM hmda_2017_balanced hm17_bal
;
/*--------------------------- end HMDA 2017 ---------------------------*/





        /*---------------------------------- Union 2016-2017 ----------------------------------*/
                ;
                WITH
                   hmda_union_2016_2017 AS
                   (
                     SELECT hm16.* FROM interim_datasets.hmda_lar_ii_2016_randsimpl_bal25k hm16
                        UNION ALL
                     SELECT hm17.* FROM interim_datasets.hmda_lar_ii_2017_randsimpl_bal25k hm17
                   )
                SELECT hm_u.*
                  INTO interim_datasets.hmda_lar_union_ii_2016_to_2017_simplerand_bal50k
                  FROM hmda_union_2016_2017 hm_u
                ;
        /*-------------------------------------------------------------------------------------*/


--> END z_ak_AWS_paddleloancanoe <---




/* Lastly, we use pg_catalogue (or dblink_connect across pgsql databases) to ingest the UNION tbls into one pgsql db */

--> hmda 2011-2013 from pgsql AWS RDS "z_tn_AWS_paddleloancanoe"
create table interim_datasets.hmda_lar_union_ii_2011_to_2013_simplerand_bal75k
     (
     	action_taken integer,
     	action_year integer,
     	tract_to_masamd_income numeric(1000,2),
     	population integer,
     	min_pop_perc numeric(1000,2),
     	num_owoc_units integer,
     	num_1to4_fam_units integer,
     	ln_amt_000s integer,
     	hud_med_fm_inc integer,
     	applic_inc_000s integer,
     	own_occ_nm varchar,
     	ln_type_nm varchar(56),
     	lien_status_nm varchar(56),
     	hoep_status_nm varchar(56),
     	co_appl_sex varchar(28),
     	co_appl_race varchar(28),
     	co_appl_ethn varchar(28),
     	applic_sex varchar(28),
     	applic_race varchar(28),
     	applic_ethn varchar(28),
     	agency_abbr varchar(28)
     )
;
set search_path = "pg_catalog"
;
set search_path = "interim_datasets"
;
SELECT CAST(reltuples as INT) as rows
FROM pg_catalog.pg_class C
LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace)
 WHERE (relkind = 'r' OR relkind = 'v')
   AND nspname LIKE 'interim#_datasets' ESCAPE '#'
   AND relname LIKE 'hmda#_lar#_union#_ii#_2011#_to#_2013#_simplerand#_bal75k' ESCAPE '#'
;
---> end of hmda 2011-2013

--> hmda 2016-2017 from pgsql AWS RDS "z_ak_AWS_paddleloancanoe"
create table interim_datasets.hmda_lar_union_ii_2016_to_2017_simplerand_bal75k
     (
     	action_taken integer,
     	action_year integer,
     	tract_to_masamd_income numeric(1000,2),
     	population integer,
     	min_pop_perc numeric(1000,2),
     	num_owoc_units integer,
     	num_1to4_fam_units integer,
     	ln_amt_000s integer,
     	hud_med_fm_inc integer,
     	applic_inc_000s integer,
     	own_occ_nm varchar,
     	ln_type_nm varchar(56),
     	lien_status_nm varchar(56),
     	hoep_status_nm varchar(56),
     	co_appl_sex varchar(28),
     	co_appl_race varchar(28),
     	co_appl_ethn varchar(28),
     	applic_sex varchar(28),
     	applic_race varchar(28),
     	applic_ethn varchar(28),
     	agency_abbr varchar(28)
     )
;
set search_path = "pg_catalog"
;
set search_path = "interim_datasets"
;
SELECT CAST(reltuples as INT) as rows
FROM pg_catalog.pg_class C
LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace)
 WHERE (relkind = 'r' OR relkind = 'v')
   AND nspname LIKE 'interim#_datasets' ESCAPE '#'
   AND relname LIKE 'hmda#_lar#_union#_ii#_2016#_to#_2017#_simplerand#_bal50k' ESCAPE '#'
;
---> end of hmda 2016-2017



        /*------------------------------------------ UNION ALL 2010-2017 ------------------------------------------*/
                ;
                WITH
                   hmda_union_2010_2017 AS
                   (
                     SELECT hm10.* FROM interim_datasets.hmda_lar_ii_2010_randsimpl_bal25k hm10
                        UNION ALL
                     SELECT hm11_13.* FROM interim_datasets.hmda_lar_union_ii_2011_to_2013_simplerand_bal75k hm11_13
                        UNION ALL
                     SELECT hm14_15.* FROM interim_datasets.hmda_lar_union_ii_2014_to_2015_simplerand_bal50k hm14_15
                        UNION ALL
                     SELECT hm16_17.* FROM interim_datasets.hmda_lar_union_ii_2016_to_2017_simplerand_bal50k hm16_17
                   )
                SELECT hm_u.*
                  INTO interim_datasets.interim_hmda_lar_union_ii_2010_to_2017_simplerand_bal200k
                  FROM hmda_union_2010_2017 hm_u
                ;
        /*---------------------------------------------------------------------------------------------------------*/

/*----------------*/


/*** =========================================== END 03b - SQL Script  ============================================ ***/
