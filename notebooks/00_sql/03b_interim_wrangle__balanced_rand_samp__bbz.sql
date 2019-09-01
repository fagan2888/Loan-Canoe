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


/*---------------------------------------------------- HMDA 2009 -----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2009_bal_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2009_bal_simplerand25k
;
--

WITH 
     hmda_2009_approved AS 
     ( SELECT
           --grouping action taken to our binary targets and typcasting all-in-one
           CAST( CASE
                      WHEN hm09.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                                      'Loan purchased by the institution' ) THEN 1
                      WHEN hm09.action_taken_name = 'Application denied by financial institution' THEN 0
                 END As INT
               )
           As action_taken, hm09.as_of_year As action_year,
           --must use embedded functions all-in-one to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm09.tract_to_msamd_income IS NULL THEN NULL ELSE hm09.tract_to_msamd_income END
                          As Varchar(5)
                      ) As NUMERIC --NB: must be numeric because numeric stores decimal places, INT is whole numbers only
               )
           As tract_to_masamd_income,
           hm09.population, ROUND(hm09.minority_population, 2) As min_pop_perc,
           hm09.number_of_owner_occupied_units As num_owoc_units, hm09.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm09.loan_amount_000s As ln_amt_000s, hm09.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm09.applicant_income_000s = '' THEN NULL ELSE hm09.applicant_income_000s END
                          As Varchar(5)
                      ) As INT
               )
           As applic_inc_000s,
          CAST(hm09.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
          CAST(hm09.loan_type_name As VARCHAR(56)) As ln_type_nm,
          CAST(hm09.lien_status_name As VARCHAR(56)) As lien_status_nm,
          CAST(hm09.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
          CAST(hm09.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
          CAST(hm09.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
          CAST(hm09.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
          CAST(hm09.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
          CAST(hm09.applicant_race_name_1 As VARCHAR(28)) As applic_race,
          CAST(hm09.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
          CAST(hm09.agency_abbr As VARCHAR(28)) As agency_abbr
    
       INTO interim_datasets.hmda_lar_2009_simplerand25k
       FROM public.hmda_lar_2009_allrecords hm09
    
      --NB: we drop the tuples with action outcomes that do not align with our targets 0 or 1
      WHERE hm09.action_taken_name
              In ( 'Application approved but not accepted', 'Application denied by financial institution',
                   'Loan originated', 'Loan purchased by the institution')
      ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2009 ---------------------------*/



/*======================== 03b. Simple Random Samples (Balanced Outcomes) for HMDA 2009-2010 =========================*/

/*** =========================================== END 03b - SQL Script  ============================================ ***/
