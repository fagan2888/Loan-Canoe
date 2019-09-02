/***************************************************************************************************************/
/* Purpose:  (1) Typecast transform & Generate Simple, Balanced, Random Samples for each year for ingestion    */
/*               + (2) Union all the samples for the interim dataset for additional wrangling with pandas      */                                                                                           */
/*                                                                                                             */
/* Author: Blake Zenuni, Summer 2019                                                                           */
/* Date Created:  Aug. 01, 2019                                                                                */
/* Last Modified: Sep. 01, 2019                                                                                */
/*                                                                                                             */
/***************************************************************************************************************/


/*---------------------------------------------------------------------------------------------------------*/
      --> NB: Latin abbreviation for NOTA BENE, meaning "note well" <--
/*---------------------------------------------------------------------------------------------------------*/


 -- NB1: In this SQL script, simple random samples are balanced for outcomes (50/50 loans approved vs. loans denied).
 -- NB2: Script 03a applies this same logic, but for unbalanced outcomes.






/*======================== 03b. Balanced Outcomes - Simple random samples for HMDA 2010-2017 =========================*/





-- Creating schema and setting users/role for accessibility profiles
CREATE SCHEMA interim_datasets ;
CREATE ROLE reporting_user WITH LOGIN PASSWORD 'team_loan_canoe2019' ;
GRANT USAGE ON SCHEMA interim_datasets TO reporting_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA interim_datasets TO reporting_user ;
--



--> z_bz_AWS_paddleloancanoe <---

/*---------------------------------------------------- HMDA 2010 -----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_ii_2010_randsimpl_bal25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_ii_2010_randsimpl_bal25k
;
--
WITH

     hmda_2010_approved As
     ( SELECT
           --simple binary assignment of 1, bc the WHERE clause in this CTE segments the approved subset
           1 As action_taken, hm10.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm10.tract_to_msamd_income IS NULL THEN NULL ELSE hm10.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC --NB: must be numeric bc numeric stores decimal places, INT is whole numbers only
               )
           As tract_to_masamd_income,
           hm10.population, ROUND(hm10.minority_population, 2) As min_pop_perc,
           hm10.number_of_owner_occupied_units As num_owoc_units,
           hm10.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm10.loan_amount_000s As ln_amt_000s, hm10.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm10.applicant_income_000s IS NULL THEN NULL ELSE hm10.applicant_income_000s END
                          As Varchar(5)
                      ) As INT
               )
           As applic_inc_000s,
          CAST(hm10.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
          CAST(hm10.loan_type_name As VARCHAR(56)) As ln_type_nm,
          CAST(hm10.lien_status_name As VARCHAR(56)) As lien_status_nm,
          CAST(hm10.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
          CAST(hm10.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
          CAST(hm10.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
          CAST(hm10.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
          CAST(hm10.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
          CAST(hm10.applicant_race_name_1 As VARCHAR(28)) As applic_race,
          CAST(hm10.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
          CAST(hm10.agency_abbr As VARCHAR(28)) As agency_abbr

       FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2010_allrecords hm10

       --NB: we drop the tuples with action outcomes that do not align with our balanced random sample of outcome=1
       WHERE hm10.action_taken_name
                 In ( 'Application approved but not accepted','Loan originated', 'Loan purchased by the institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2010_denied As
     ( SELECT
           --simple binary assignment of 0, bc the WHERE clause in this CTE segments the denied subset
           0 As action_taken, hm10.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm10.tract_to_msamd_income IS NULL THEN NULL ELSE hm10.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC --NB: must be numeric bc numeric stores decimal places, INT is whole numbers only
               )
           As tract_to_masamd_income,
           hm10.population, ROUND(hm10.minority_population, 2) As min_pop_perc,
           hm10.number_of_owner_occupied_units As num_owoc_units,
           hm10.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm10.loan_amount_000s As ln_amt_000s, hm10.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm10.applicant_income_000s IS NULL THEN NULL ELSE hm10.applicant_income_000s END
                          As Varchar(5)
                      ) As INT
               )
           As applic_inc_000s,
          CAST(hm10.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
          CAST(hm10.loan_type_name As VARCHAR(56)) As ln_type_nm,
          CAST(hm10.lien_status_name As VARCHAR(56)) As lien_status_nm,
          CAST(hm10.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
          CAST(hm10.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
          CAST(hm10.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
          CAST(hm10.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
          CAST(hm10.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
          CAST(hm10.applicant_race_name_1 As VARCHAR(28)) As applic_race,
          CAST(hm10.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
          CAST(hm10.agency_abbr As VARCHAR(28)) As agency_abbr

       FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2010_allrecords hm10

       --NB: we drop the tuples with action outcomes that do not align with our balanced random sample of outcome=1
       WHERE hm10.action_taken_name  In ('Application denied by financial institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2010_balanced AS
     ( SELECT hm10_app.* From hmda_2010_approved hm10_app
            UNION ALL
       SELECT hm10_den.* FROM hmda_2010_denied hm10_den
     )

SELECT hm10_bal.*
INTO interim_datasets.hmda_lar_ii_2010_randsimpl_bal25k
FROM hmda_2010_balanced hm10_bal
;
/*--------------------------- end HMDA 2010 ---------------------------*/

--> END z_bz_AWS_paddleloancanoe <---





/* ==> NB: All subsequent HMDA individual years will apply the same SQL logic from the code above, but will be stripped
       of comments for length
*/





---> z_tn_AWS_paddleloancanoe <---

/*---------------------------------------------------- HMDA 2011 -----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_ii_2011_randsimpl_bal25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_ii_2011_randsimpl_bal25k
;
--
WITH

     hmda_2011_approved As
     ( SELECT
           1 As action_taken, hm11.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm11.tract_to_msamd_income IS NULL THEN NULL ELSE hm11.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm11.population, ROUND(hm11.minority_population, 2) As min_pop_perc,
           hm11.number_of_owner_occupied_units As num_owoc_units,
           hm11.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm11.loan_amount_000s As ln_amt_000s, hm11.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm11.applicant_income_000s IS NULL THEN NULL ELSE hm11.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm11.owner_occupancy_name As VARCHAR(118)) As own_occ_nm,
           CAST(hm11.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm11.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm11.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm11.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm11.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm11.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm11.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm11.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm11.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm11.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2011_allrecords hm11
       WHERE hm11.action_taken_name
                 In ( 'Application approved but not accepted','Loan originated', 'Loan purchased by the institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2011_denied As
     ( SELECT
           0 As action_taken, hm11.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm11.tract_to_msamd_income IS NULL THEN NULL ELSE hm11.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm11.population, ROUND(hm11.minority_population, 2) As min_pop_perc,
           hm11.number_of_owner_occupied_units As num_owoc_units,
           hm11.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm11.loan_amount_000s As ln_amt_000s, hm11.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm11.applicant_income_000s IS NULL THEN NULL ELSE hm11.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm11.owner_occupancy_name As VARCHAR(118)) As own_occ_nm,
           CAST(hm11.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm11.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm11.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm11.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm11.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm11.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm11.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm11.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm11.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm11.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2011_allrecords hm11
       WHERE hm11.action_taken_name  In ('Application denied by financial institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2011_balanced AS
     ( SELECT hm11_app.* From hmda_2011_approved hm11_app
            UNION ALL
       SELECT hm11_den.* FROM hmda_2011_denied hm11_den
     )

SELECT hm11_bal.*
INTO interim_datasets.hmda_lar_ii_2011_randsimpl_bal25k
FROM hmda_2011_balanced hm11_bal
;
/*--------------------------- end HMDA 2011 ---------------------------*/




/*----------------------------------------------------- HMDA 2012 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_ii_2012_randsimpl_bal25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_ii_2012_randsimpl_bal25k
;
--
WITH

     hmda_2012_approved As
     ( SELECT
           1 As action_taken, hm12.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm12.tract_to_msamd_income IS NULL THEN NULL ELSE hm12.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm12.population, ROUND(hm12.minority_population, 2) As min_pop_perc,
           hm12.number_of_owner_occupied_units As num_owoc_units,
           hm12.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm12.loan_amount_000s As ln_amt_000s, hm12.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm12.applicant_income_000s IS NULL THEN NULL ELSE hm12.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm12.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm12.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm12.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm12.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm12.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm12.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm12.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm12.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm12.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2012_allrecords hm12
       WHERE hm12.action_taken_name
                 In ( 'Application approved but not accepted','Loan originated', 'Loan purchased by the institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2012_denied As
     ( SELECT
           0 As action_taken, hm12.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm12.tract_to_msamd_income IS NULL THEN NULL ELSE hm12.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm12.population, ROUND(hm12.minority_population, 2) As min_pop_perc,
           hm12.number_of_owner_occupied_units As num_owoc_units,
           hm12.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm12.loan_amount_000s As ln_amt_000s, hm12.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm12.applicant_income_000s IS NULL THEN NULL ELSE hm12.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm12.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm12.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm12.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm12.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm12.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm12.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm12.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm12.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm12.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2012_allrecords hm12
       WHERE hm12.action_taken_name  In ('Application denied by financial institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2012_balanced AS
     ( SELECT hm12_app.* From hmda_2012_approved hm12_app
            UNION ALL
       SELECT hm12_den.* FROM hmda_2012_denied hm12_den
     )

SELECT hm12_bal.*
INTO interim_datasets.hmda_lar_ii_2012_randsimpl_bal25k
FROM hmda_2012_balanced hm12_bal
;
/*--------------------------- end HMDA 2012 ---------------------------*/




/*----------------------------------------------------- HMDA 2013 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_ii_2013_randsimpl_bal25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_ii_2013_randsimpl_bal25k
;
--
WITH

     hmda_2013_approved As
     ( SELECT
           1 As action_taken, hm13.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm13.tract_to_msamd_income IS NULL THEN NULL ELSE hm13.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm13.population, ROUND(hm13.minority_population, 2) As min_pop_perc,
           hm13.number_of_owner_occupied_units As num_owoc_units,
           hm13.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm13.loan_amount_000s As ln_amt_000s, hm13.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm13.applicant_income_000s IS NULL THEN NULL ELSE hm13.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm13.owner_occupancy_name As VARCHAR(138)) As own_occ_nm,
           CAST(hm13.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm13.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm13.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm13.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm13.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm13.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm13.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm13.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm13.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm13.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2013_allrecords hm13
       WHERE hm13.action_taken_name
                 In ( 'Application approved but not accepted','Loan originated', 'Loan purchased by the institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2013_denied As
     ( SELECT
           0 As action_taken, hm13.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm13.tract_to_msamd_income IS NULL THEN NULL ELSE hm13.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm13.population, ROUND(hm13.minority_population, 2) As min_pop_perc,
           hm13.number_of_owner_occupied_units As num_owoc_units,
           hm13.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm13.loan_amount_000s As ln_amt_000s, hm13.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm13.applicant_income_000s IS NULL THEN NULL ELSE hm13.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm13.owner_occupancy_name As VARCHAR(138)) As own_occ_nm,
           CAST(hm13.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm13.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm13.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm13.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm13.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm13.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm13.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm13.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm13.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm13.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2013_allrecords hm13
       WHERE hm13.action_taken_name  In ('Application denied by financial institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2013_balanced AS
     ( SELECT hm13_app.* From hmda_2013_approved hm13_app
            UNION ALL
       SELECT hm13_den.* FROM hmda_2013_denied hm13_den
     )

SELECT hm13_bal.*
INTO interim_datasets.hmda_lar_ii_2013_randsimpl_bal25k
FROM hmda_2013_balanced hm13_bal
:
/*--------------------------- end HMDA 2013 ---------------------------*/




        /*---------------------------------- Union 2011-2013 ----------------------------------*/
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
                  INTO interim_datasets.hmda_lar_ii_union_2011_to_2013_simplerand75k
                  FROM hmda_union_2011_2013 hm_u
                ;
        /*-------------------------------------------------------------------------------------*/


--> END z_tn_AWS_paddleloancanoe <---





--> z_bz_AWS_paddleloancanoe <---

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
           CAST( CAST( CASE WHEN hm14.applicant_income_000s IS NULL THEN NULL ELSE hm14.applicant_income_000s END
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
       FROM public.hmda_lar_2014_allrecords hm14
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
           CAST( CAST( CASE WHEN hm14.applicant_income_000s IS NULL THEN NULL ELSE hm14.applicant_income_000s END
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
       FROM public.hmda_lar_2014_allrecords hm14
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
:
/*--------------------------- end HMDA 2014 ---------------------------*/



/*----------------------------------------------------- HMDA 2015 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_ii_2015_randsimpl_bal25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_ii_2015_randsimpl_bal25k
;
--
WITH

     hmda_2015_approved As
     ( SELECT
           1 As action_taken, hm15.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm15.tract_to_msamd_income IS NULL THEN NULL ELSE hm15.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm15.population, ROUND(hm15.minority_population, 2) As min_pop_perc,
           hm15.number_of_owner_occupied_units As num_owoc_units,
           hm15.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm15.loan_amount_000s As ln_amt_000s, hm15.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm15.applicant_income_000s IS NULL THEN NULL ELSE hm15.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm15.owner_occupancy_name As VARCHAR(158)) As own_occ_nm,
           CAST(hm15.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm15.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm15.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm15.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm15.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm15.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm15.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm15.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm15.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm15.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2015_allrecords hm15
       WHERE hm15.action_taken_name
                 In ( 'Application approved but not accepted','Loan originated', 'Loan purchased by the institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2015_denied As
     ( SELECT
           0 As action_taken, hm15.as_of_year As action_year,
           CAST( CAST( CASE WHEN hm15.tract_to_msamd_income IS NULL THEN NULL ELSE hm15.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC
               )
           As tract_to_masamd_income,
           hm15.population, ROUND(hm15.minority_population, 2) As min_pop_perc,
           hm15.number_of_owner_occupied_units As num_owoc_units,
           hm15.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm15.loan_amount_000s As ln_amt_000s, hm15.hud_median_family_income As hud_med_fm_inc,
           CAST( CAST( CASE WHEN hm15.applicant_income_000s IS NULL THEN NULL ELSE hm15.applicant_income_000s END
                          As Varchar(5)) As INT) As applic_inc_000s,
           CAST(hm15.owner_occupancy_name As VARCHAR(158)) As own_occ_nm,
           CAST(hm15.loan_type_name As VARCHAR(56)) As ln_type_nm,
           CAST(hm15.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm15.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm15.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm15.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm15.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm15.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm15.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm15.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm15.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM public.hmda_lar_2015_allrecords hm15
       WHERE hm15.action_taken_name  In ('Application denied by financial institution')
       ORDER BY random() LIMIT 12500
     ) ,

     hmda_2015_balanced AS
     ( SELECT hm15_app.* From hmda_2015_approved hm15_app
            UNION ALL
       SELECT hm15_den.* FROM hmda_2015_denied hm15_den
     )

SELECT hm15_bal.*
INTO interim_datasets.hmda_lar_ii_2015_randsimpl_bal25k
FROM hmda_2015_balanced hm15_bal
:
/*--------------------------- end HMDA 2015 ---------------------------*/




        /*---------------------------------- Union 2014-2015 ----------------------------------*/
                ;
                WITH
                   hmda_union_2014_2015 AS
                   (
                     SELECT hm14.* FROM interim_datasets.hmda_lar_ii_2011_randsimpl_bal25k hm11
                        UNION ALL
                     SELECT hm15.* FROM interim_datasets.hmda_lar_ii_2012_randsimpl_bal25k hm12
                   )
                SELECT hm_u.*
                  INTO interim_datasets.hmda_lar_ii_union_2014_to_2015_simplerand50k
                  FROM hmda_union_2014_2015 hm_u
                ;
        /*-------------------------------------------------------------------------------------*/


--> END z_bz_AWS_paddleloancanoe <---





--> z_ak_AWS_paddleloancanoe <---

/*----------------------------------------------------- HMDA 2016 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2016_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2016_simplerand25k
;
SELECT
  CAST( CASE WHEN hm16.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                              'Loan purchased by the institution' ) THEN 1
             WHEN hm16.action_taken_name = 'Application denied by financial institution' THEN 0
        END As INT) As action_taken, hm16.as_of_year As action_year,
  CAST( CAST( CASE WHEN hm16.tract_to_msamd_income IS NULL THEN NULL ELSE hm16.tract_to_msamd_income END
                As Varchar(5)) As NUMERIC ) As tract_to_masamd_income, hm16.population,
  ROUND(hm16.minority_population, 2) As min_pop_perc, hm16.number_of_owner_occupied_units As num_owoc_units,
  hm16.number_of_1_to_4_family_units As num_1to4_fam_units, hm16.loan_amount_000s As ln_amt_000s,
  hm16.hud_median_family_income As hud_med_fm_inc,
  CAST( CAST( CASE WHEN hm16.applicant_income_000s = '' THEN NULL ELSE hm16.applicant_income_000s END
                As Varchar(5) ) As INT) As applic_inc_000s,
  CAST(hm16.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
  CAST(hm16.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm16.lien_status_name As VARCHAR(56)) As lien_status_nm,
  CAST(hm16.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
  CAST(hm16.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
  CAST(hm16.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
  CAST(hm16.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
  CAST(hm16.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
  CAST(hm16.applicant_race_name_1 As VARCHAR(28)) As applic_race,
  CAST(hm16.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
  CAST(hm16.agency_abbr As VARCHAR(28)) As agency_abbr
  INTO interim_datasets.hmda_lar_2016_simplerand25k
  FROM hmda_lar_2016_allrecords hm16
  WHERE hm16.action_taken_name
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution')
  ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2016 ---------------------------*/



/*----------------------------------------------------- HMDA 2017 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2017_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2017_simplerand25k
;
SELECT
  CAST( CASE WHEN hm17.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                              'Loan purchased by the institution' ) THEN 1
             WHEN hm17.action_taken_name = 'Application denied by financial institution' THEN 0
        END As INT) As action_taken, hm17.as_of_year As action_year,
  CAST( CAST( CASE WHEN hm17.tract_to_msamd_income IS NULL THEN NULL ELSE hm17.tract_to_msamd_income END
                As Varchar(5)) As NUMERIC ) As tract_to_masamd_income, hm17.population,
  ROUND(hm17.minority_population, 2) As min_pop_perc, hm17.number_of_owner_occupied_units As num_owoc_units,
  hm17.number_of_1_to_4_family_units As num_1to4_fam_units, hm17.loan_amount_000s As ln_amt_000s,
  hm17.hud_median_family_income As hud_med_fm_inc,
  CAST( CAST( CASE WHEN hm17.applicant_income_000s = '' THEN NULL ELSE hm17.applicant_income_000s END
                As Varchar(5) ) As INT) As applic_inc_000s,
  CAST(hm17.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
  CAST(hm17.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm17.lien_status_name As VARCHAR(56)) As lien_status_nm,
  CAST(hm17.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
  CAST(hm17.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
  CAST(hm17.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
  CAST(hm17.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
  CAST(hm17.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
  CAST(hm17.applicant_race_name_1 As VARCHAR(28)) As applic_race,
  CAST(hm17.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
  CAST(hm17.agency_abbr As VARCHAR(28)) As agency_abbr
  INTO interim_datasets.hmda_lar_2017_simplerand25k
  FROM hmda_lar_2017_allrecords hm17
  WHERE hm17.action_taken_name
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution')
  ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2017 ---------------------------*/




        /*---------------------------------- Union 2016-2017 ----------------------------------*/
                ;
                WITH
                   hmda_union_2016_2017 AS
                   (
                     SELECT hm16.* FROM interim_datasets.hmda_lar_2016_simplerand25k hm16
                        UNION ALL
                     SELECT hm17.* FROM interim_datasets.hmda_lar_2017_simplerand25k hm17
                   )
                SELECT hm_u.*
                  INTO interim_datasets.hmda_lar_union_2016_to_2017_simplerand50k
                  FROM hmda_union_2016_2017 hm_u
                ;
        /*-------------------------------------------------------------------------------------*/


--> END z_ak_AWS_paddleloancanoe <---





/*** =========================================== END 03b - SQL Script  ============================================ ***/
