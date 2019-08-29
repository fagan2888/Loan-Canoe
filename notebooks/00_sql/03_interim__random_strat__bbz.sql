/***********************************************************************************************************/
/* Purpose:  (1) Generate Random Samples (simple and stratified) for each year for ingestion               */
/*               + (2) Union all the samples for the interim dataset for additional wrangling with pandas  */                                                                                           */
/*                                                                                                         */
/* Author: Blake Zenuni, Summer 2019                                                                       */
/* Date Created:  Aug 1, 2019                                                                              */
/* Last Modified: Aub 28, 2019                                                                             */
/*                                                                                                         */
/***********************************************************************************************************/


/*---------------------------------------------------------------------------------------------------------*/
      --> NB: Latin abbreviation for NOTA BENE, meaning "note well" <--
/*---------------------------------------------------------------------------------------------------------*/


 -- NB1: Simple random samples are unbalanced for outcomes
 -- NB2: Stratified random samples are bucketed by household income (strata) + balanced two ways (see below)



/*------------------------------------------------------------*/

/****** A. Simple random samples for HMDA 2009 - 2010 ********/

------ i. HMDA 2009 -----

  --> generate distinct list of action taken names for grouping
Select Distinct hm09.action_taken_name From hmda_lar_2009_allrecords hm09
;
SELECT --grouping action taken to our binary targets and typcasting all-in-one
       CAST( CASE
                  WHEN hm09.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                                  'Loan purchased by the institution' ) THEN 1
                  WHEN hm09.action_taken_name = 'Application denied by financial institution' THEN 0
             END As INT
           )
       As action_taken,
       --must use embedded functions all-in-one to typecast this as integer because of NULL/space characters in raw data
       CAST( CAST( CASE WHEN hm09.tract_to_msamd_income IS NULL THEN NULL ELSE hm09.tract_to_msamd_income END
                      As Varchar(5)
                  ) As NUMERIC --NB: must be numeric because numeric stores decimal places, INT is whole numbers only
           )
       As tract_to_masamd_income,
       hm09.population, hm09.minority_population As min_pop_perc, hm09.number_of_owner_occupied_units As num_owoc_units,
       hm09.number_of_1_to_4_family_units As num_1to4_fam_units, hm09.loan_amount_000s As ln_amt_000s,
       hm09.hud_median_family_income As hud_med_fm_inc,
       --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
       CAST( CAST( CASE WHEN hm09.applicant_income_000s = '' THEN NULL ELSE hm09.applicant_income_000s END
                      As Varchar(5)
                  ) As INT
           )
       As applic_inc_000s,
      CAST(hm09.owner_occupancy_name As VARCHAR(128))

  FROM hmda_lar_2009_allrecords hm09
  WHERE hm09.action_taken_name --NB: we drop the tuples with action outcomes that do not align with our targets 0 or 1
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution') LIMIT 5
;
----- end i. HMDA 2009 -----

/*---------------*/


/*** ============================================== END SQL Script  =============================================== ***/

SELECT DISTINCT action_taken_name from hmda_lar_2009_allrecords ;
select * from hmda_lar_2009_allrecords where tract_to_msamd_income ;
select applicant_income_000s from hmda_lar_2009_allrecords limit 10 ;

CAST( CAST( CASE WHEN hm09.tract_to_msamd_income = '' ELSE hm09.tract_to_msamd_income END
                      As varchar(5)
                  ) As NUMERIC --NB: must be numeric because numeric stores decimal places, INT is whole numbers only
           )
       As tract_to_masamd_income,