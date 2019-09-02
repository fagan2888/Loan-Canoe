/**********************************************************************************************************************/
/* Purpose:  (1) Typecast transform & Generate Simple, Unbalanced Random Samples for each year for ingestion          */
/*               + (2) Union all the samples for the interim dataset for additional wrangling with pandas             */                                                                                           */
/*                                                                                                                    */
/* Author: Blake Zenuni, Summer 2019                                                                                  */
/* Date Created:  Aug 01, 2019                                                                                        */
/* Last Modified: Aub 30, 2019                                                                                       */
/*                                                                                                                    */
/**********************************************************************************************************************/


/*---------------------------------------------------------------------------------------------------------*/
      --> NB: Latin abbreviation for NOTA BENE, meaning "note well" <--
/*---------------------------------------------------------------------------------------------------------*/


 -- NB1: In this SQL script, simple random samples are unbalanced for outcomes.
 -- NB2: Script 03b applies this same logic, but for balanced outcomes (50/50 loans approved vs. loans denied).






/*================================== 03a. Simple random samples for HMDA 2010-2017 ===================================*/





-- Creating schema and setting users/role for accessibility profiles
CREATE SCHEMA interim_datasets ;
CREATE ROLE reporting_user WITH LOGIN PASSWORD 'team_loan_canoe2019' ;
GRANT USAGE ON SCHEMA interim_datasets TO reporting_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA interim_datasets TO reporting_user ;
--



--> z_bz_AWS_paddleloancanoe <---

/*---------------------------------------------------- HMDA 2010 -----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2010_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2010_simplerand25k
;
--
SELECT
       --grouping action taken to our binary targets and typcasting all-in-one
       CAST( CASE
                  WHEN hm10.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                                  'Loan purchased by the institution' ) THEN 1
                  WHEN hm10.action_taken_name = 'Application denied by financial institution' THEN 0
             END As INT
           )
       As action_taken, hm10.as_of_year As action_year,
       --must use embedded functions all-in-one to typecast this as integer because of NULL/space characters in raw data
       CAST( CAST( CASE WHEN hm10.tract_to_msamd_income IS NULL THEN NULL ELSE hm10.tract_to_msamd_income END
                      As Varchar(5)
                  ) As NUMERIC --NB: must be numeric because numeric stores decimal places, INT is whole numbers only
           )
       As tract_to_masamd_income,
       hm10.population, ROUND(hm10.minority_population, 2) As min_pop_perc,
       hm10.number_of_owner_occupied_units As num_owoc_units, hm10.number_of_1_to_4_family_units As num_1to4_fam_units,
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

  INTO interim_datasets.hmda_lar_2010_simplerand25k
  FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2010_allrecords hm10

  --NB: we drop the tuples with action outcomes that do not align with our targets 0 or 1
  WHERE hm10.action_taken_name
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution')
  ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2010 ---------------------------*/

--> END z_bz_AWS_paddleloancanoe <---





/* ==> NB: All subsequent HMDA individual years will apply the same SQL logic from the code above, but will be stripped
       of comments for length
*/





---> z_tn_AWS_paddleloancanoe <---

/*----------------------------------------------------- HMDA 2011 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2011_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2011_simplerand25k
;
SELECT
  CAST( CASE WHEN hm11.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                              'Loan purchased by the institution' ) THEN 1
             WHEN hm11.action_taken_name = 'Application denied by financial institution' THEN 0
        END As INT) As action_taken, hm11.as_of_year As action_year,
  CAST( CAST( CASE WHEN hm11.tract_to_msamd_income IS NULL THEN NULL ELSE hm11.tract_to_msamd_income END
                As Varchar(5)) As NUMERIC ) As tract_to_masamd_income, hm11.population,
  ROUND(hm11.minority_population, 2) As min_pop_perc, hm11.number_of_owner_occupied_units As num_owoc_units,
  hm11.number_of_1_to_4_family_units As num_1to4_fam_units, hm11.loan_amount_000s As ln_amt_000s,
  hm11.hud_median_family_income As hud_med_fm_inc,
  CAST( CAST( CASE WHEN hm11.applicant_income_000s IS NULL THEN NULL ELSE hm11.applicant_income_000s END
                As Varchar(5) ) As INT) As applic_inc_000s,
  CAST(hm11.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
  CAST(hm11.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm11.lien_status_name As VARCHAR(56)) As lien_status_nm,
  CAST(hm11.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
  CAST(hm11.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
  CAST(hm11.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
  CAST(hm11.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
  CAST(hm11.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
  CAST(hm11.applicant_race_name_1 As VARCHAR(28)) As applic_race,
  CAST(hm11.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
  CAST(hm11.agency_abbr As VARCHAR(28)) As agency_abbr
  INTO interim_datasets.hmda_lar_2011_simplerand25k
  FROM public.hmda_lar_2011_allrecords hm11
  WHERE hm11.action_taken_name
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution')
  ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2011 ---------------------------*/




/*----------------------------------------------------- HMDA 2012 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2012_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2012_simplerand25k
;
SELECT
  CAST( CASE WHEN hm12.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                              'Loan purchased by the institution' ) THEN 1
             WHEN hm12.action_taken_name = 'Application denied by financial institution' THEN 0
        END As INT) As action_taken, hm12.as_of_year As action_year,
  CAST( CAST( CASE WHEN hm12.tract_to_msamd_income IS NULL THEN NULL ELSE hm12.tract_to_msamd_income END
                As Varchar(5)) As NUMERIC ) As tract_to_masamd_income, hm12.population,
  ROUND(hm12.minority_population, 2) As min_pop_perc, hm12.number_of_owner_occupied_units As num_owoc_units,
  hm12.number_of_1_to_4_family_units As num_1to4_fam_units, hm12.loan_amount_000s As ln_amt_000s,
  hm12.hud_median_family_income As hud_med_fm_inc,
  CAST( CAST( CASE WHEN hm12.applicant_income_000s IS NULL THEN NULL ELSE hm12.applicant_income_000s END
                As Varchar(5) ) As INT) As applic_inc_000s,
  CAST(hm12.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
  CAST(hm12.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm12.lien_status_name As VARCHAR(56)) As lien_status_nm,
  CAST(hm12.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
  CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
  CAST(hm12.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
  CAST(hm12.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
  CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
  CAST(hm12.applicant_race_name_1 As VARCHAR(28)) As applic_race,
  CAST(hm12.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
  CAST(hm12.agency_abbr As VARCHAR(28)) As agency_abbr
  INTO interim_datasets.hmda_lar_2012_simplerand25k
  FROM public.hmda_lar_2012_allrecords hm12
  WHERE hm12.action_taken_name
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution')
  ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2012 ---------------------------*/




/*----------------------------------------------------- HMDA 2013 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2013_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2013_simplerand25k
;
SELECT
  CAST( CASE WHEN hm13.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                              'Loan purchased by the institution' ) THEN 1
             WHEN hm13.action_taken_name = 'Application denied by financial institution' THEN 0
        END As INT) As action_taken, hm13.as_of_year As action_year,
  CAST( CAST( CASE WHEN hm13.tract_to_msamd_income IS NULL THEN NULL ELSE hm13.tract_to_msamd_income END
                As Varchar(5)) As NUMERIC ) As tract_to_masamd_income, hm13.population,
  ROUND(hm13.minority_population, 2) As min_pop_perc, hm13.number_of_owner_occupied_units As num_owoc_units,
  hm13.number_of_1_to_4_family_units As num_1to4_fam_units, hm13.loan_amount_000s As ln_amt_000s,
  hm13.hud_median_family_income As hud_med_fm_inc,
  CAST( CAST( CASE WHEN hm13.applicant_income_000s IS NULL THEN NULL ELSE hm13.applicant_income_000s END
                As Varchar(5) ) As INT) As applic_inc_000s,
  CAST(hm13.owner_occupancy_name As VARCHAR(138)) As own_occ_nm,
  CAST(hm13.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm13.lien_status_name As VARCHAR(56)) As lien_status_nm,
  CAST(hm13.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
  CAST(hm13.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
  CAST(hm13.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
  CAST(hm13.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
  CAST(hm13.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
  CAST(hm13.applicant_race_name_1 As VARCHAR(28)) As applic_race,
  CAST(hm13.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
  CAST(hm13.agency_abbr As VARCHAR(28)) As agency_abbr
  INTO interim_datasets.hmda_lar_2013_simplerand25k
  FROM public.hmda_lar_2013_allrecords hm13
  WHERE hm13.action_taken_name
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution')
  ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2013 ---------------------------*/




        /*---------------------------------- Union 2011-2013 ----------------------------------*/
                ;
                WITH
                   hmda_union_2011_2013 AS
                   (
                     SELECT hm11.* FROM interim_datasets.hmda_lar_2011_simplerand25k hm11
                        UNION ALL
                     SELECT hm12.* FROM interim_datasets.hmda_lar_2012_simplerand25k hm12
                        UNION ALL
                     SELECT hm13.* FROM interim_datasets.hmda_lar_2013_simplerand25k hm13
                   )
                SELECT hm_u.*
                  INTO interim_datasets.hmda_lar_union_2011_to_2013_simplerand75k
                  FROM hmda_union_2011_2013 hm_u
                ;
        /*-------------------------------------------------------------------------------------*/


--> END z_tn_AWS_paddleloancanoe <---





--> z_bz_AWS_paddleloancanoe <---

/*----------------------------------------------------- HMDA 2014 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2014_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2014_simplerand25k
;
SELECT
  CAST( CASE WHEN hm14.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                              'Loan purchased by the institution' ) THEN 1
             WHEN hm14.action_taken_name = 'Application denied by financial institution' THEN 0
        END As INT) As action_taken, hm14.as_of_year As action_year,
  CAST( CAST( CASE WHEN hm14.tract_to_msamd_income IS NULL THEN NULL ELSE hm14.tract_to_msamd_income END
                As Varchar(5)) As NUMERIC ) As tract_to_masamd_income, hm14.population,
  ROUND(hm14.minority_population, 2) As min_pop_perc, hm14.number_of_owner_occupied_units As num_owoc_units,
  hm14.number_of_1_to_4_family_units As num_1to4_fam_units, hm14.loan_amount_000s As ln_amt_000s,
  hm14.hud_median_family_income As hud_med_fm_inc,
  CAST( CAST( CASE WHEN hm14.applicant_income_000s IS NULL THEN NULL ELSE hm14.applicant_income_000s END
                As Varchar(5) ) As INT) As applic_inc_000s,
  CAST(hm14.owner_occupancy_name As VARCHAR(148)) As own_occ_nm,
  CAST(hm14.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm14.lien_status_name As VARCHAR(56)) As lien_status_nm,
  CAST(hm14.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
  CAST(hm14.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
  CAST(hm14.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
  CAST(hm14.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
  CAST(hm14.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
  CAST(hm14.applicant_race_name_1 As VARCHAR(28)) As applic_race,
  CAST(hm14.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
  CAST(hm14.agency_abbr As VARCHAR(28)) As agency_abbr
  INTO interim_datasets.hmda_lar_2014_simplerand25k
  FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2014_allrecords hm14
  WHERE hm14.action_taken_name
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution')
  ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2014 ---------------------------*/



/*----------------------------------------------------- HMDA 2015 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2014_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2014_simplerand25k
;
SELECT
  CAST( CASE WHEN hm14.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                              'Loan purchased by the institution' ) THEN 1
             WHEN hm14.action_taken_name = 'Application denied by financial institution' THEN 0
        END As INT) As action_taken, hm14.as_of_year As action_year,
  CAST( CAST( CASE WHEN hm14.tract_to_msamd_income IS NULL THEN NULL ELSE hm14.tract_to_msamd_income END
                As Varchar(5)) As NUMERIC ) As tract_to_masamd_income, hm14.population,
  ROUND(hm14.minority_population, 2) As min_pop_perc, hm14.number_of_owner_occupied_units As num_owoc_units,
  hm14.number_of_1_to_4_family_units As num_1to4_fam_units, hm14.loan_amount_000s As ln_amt_000s,
  hm14.hud_median_family_income As hud_med_fm_inc,
  CAST( CAST( CASE WHEN hm14.applicant_income_000s IS NULL THEN NULL ELSE hm14.applicant_income_000s END
                As Varchar(5) ) As INT) As applic_inc_000s,
  CAST(hm14.owner_occupancy_name As VARCHAR(148)) As own_occ_nm,
  CAST(hm14.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm14.lien_status_name As VARCHAR(56)) As lien_status_nm,
  CAST(hm14.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
  CAST(hm14.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
  CAST(hm14.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
  CAST(hm14.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
  CAST(hm14.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
  CAST(hm14.applicant_race_name_1 As VARCHAR(28)) As applic_race,
  CAST(hm14.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
  CAST(hm14.agency_abbr As VARCHAR(28)) As agency_abbr
  --INTO interim_datasets.hmda_lar_2014_simplerand25k
  FROM usa_mortgage_market.hmda_lar_2014_allrecords hm14
  WHERE hm14.action_taken_name
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution')
  ORDER BY random() LIMIT 5
;
/*--------------------------- end HMDA 2015 ---------------------------*/




        /*---------------------------------- Union 2014-2015 ----------------------------------*/
                ;
                WITH
                   hmda_union_2014_2015 AS
                   (
                     SELECT hm14.* FROM interim_datasets.hmda_lar_2014_simplerand25k hm14
                        UNION ALL
                     SELECT hm15.* FROM interim_datasets.hmda_lar_2015_simplerand25k hm15
                   )
                SELECT hm_u.*
                  INTO interim_datasets.hmda_lar_union_2014_to_2015_simplerand50k
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
   AND relname LIKE 'hmda#_lar#_union#_2011#_to#_2013#_simplerand75k' ESCAPE '#'
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
   AND relname LIKE 'hmda#_lar#_union#_2016#_to#_2017#_simplerand50k' ESCAPE '#'
;
---> end of hmda 2016-2017


        /*------------------------------------------ UNION ALL 2010-2017 ------------------------------------------*/
                ;
                WITH
                   hmda_union_2010_2017 AS
                   (
                     SELECT hm10.* FROM interim_datasets.hmda_lar_2010_simplerand25k hm10
                        UNION ALL
                     SELECT hm11_13.* FROM interim_datasets.hmda_lar_union_2011_to_2013_simplerand75k hm11_13
                        UNION ALL
                     SELECT hm14_15.* FROM interim_datasets.hmda_lar_union_2014_to_2015_simplerand50k hm14_15
                        UNION ALL
                     SELECT hm16_17.* FROM interim_datasets.hmda_lar_union_2016_to_2017_simplerand50k hm16_17
                   )
                SELECT hm_u.*
                  INTO interim_datasets.interim_hmda_lar_union_2010_to_2017_simplerand200k
                  FROM hmda_union_2010_2017 hm_u
                ;
        /*---------------------------------------------------------------------------------------------------------*/

/*----------------*/


/*** =========================================== END 03a - SQL Script  ============================================ ***/
