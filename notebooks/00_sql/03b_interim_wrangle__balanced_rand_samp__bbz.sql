/***************************************************************************************************************/
/* Purpose:  (1) Typecast transform & Generate Simple, Balanced, Random Samples for each year for ingestion    */
/*               + (2) Union all the samples for the interim dataset for additional wrangling with pandas      */                                                                                           */
/*                                                                                                             */
/* Author: Blake Zenuni, Summer 2019                                                                           */
/* Date Created:  Aug. 01, 2019                                                                                */
/* Last Modified: Sep. 06, 2019                                                                                */
/*                                                                                                             */
/***************************************************************************************************************/


/*---------------------------------------------------------------------------------------------------------*/
      --> NB: Latin abbreviation for NOTA BENE, meaning "note well" <--
/*---------------------------------------------------------------------------------------------------------*/


 -- NB1: In this SQL script, simple random samples are balanced for outcomes (50/50 loans approved vs. loans denied).
 -- NB2: Script 03a applies this same logic, but for unbalanced outcomes.






/*======================== 03b. Balanced Outcomes - Simple random samples for HMDA 2010-2017 =========================*/





/*--Creating schema and setting users/role for accessibility profiles--*/
CREATE EXTENSION dblink ;
CREATE SCHEMA interim_datasets ;
CREATE ROLE reporting_user WITH LOGIN PASSWORD 'team_loan_canoe2019' ;
GRANT USAGE ON SCHEMA interim_datasets TO reporting_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA interim_datasets TO reporting_user ;
--
CREATE SCHEMA interim_datasets_v2 ;
GRANT USAGE ON SCHEMA interim_datasets TO reporting_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA interim_datasets TO reporting_user ;
/*------------*/




--> z_bz_AWS_paddleloancanoe <---


/*----------------------------------------------------- HMDA 2010 ----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets_v2.hmda10_srandom_bal_25K ;
--
WITH
     hm_10_ap AS --extract the raw data fields for segment of approved loans with loan type = Conventional
     ( SELECT hm10.action_taken_name As act, hm10.as_of_year, TRIM(LEADING '0' FROM rate_spread) As rate_spread,
              hm10.tract_to_msamd_income, hm10.population, hm10.agency_abbr, hm10.minority_population,
              hm10.number_of_owner_occupied_units, hm10.loan_amount_000s, hm10.number_of_1_to_4_family_units,
              hm10.hud_median_family_income, hm10.applicant_income_000s, hm10.state_abbr, hm10.property_type_name,
              hm10.owner_occupancy_name, hm10.msamd_name, hm10.lien_status_name, hm10.hoepa_status_name,
              hm10.co_applicant_sex_name, hm10.co_applicant_ethnicity_name, hm10.co_applicant_race_name_1,
              hm10.applicant_sex_name, hm10.applicant_race_name_1, hm10.applicant_ethnicity_name,
              --set Null because all denial reasons are null for approved loans
              NULL As denial_reason_name_1, NULL AS denial_reason_name_2, NULL AS denial_reason_name_3
       FROM usa_mortgage_market.hmda_lar_2010_allrecords hm10
       WHERE hm10.action_taken_name = 'Loan originated'
         AND hm10.loan_type_name = 'Conventional'
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm10.as_of_year Is Not NULL                      AND    hm10.tract_to_msamd_income Is Not Null        AND
               hm10.population Is Not Null                      AND    hm10.minority_population Is Not Null          AND
               hm10.number_of_owner_occupied_units Is Not Null  AND    hm10.hud_median_family_income Is Not Null     AND
               hm10.applicant_income_000s Is Not Null           AND    hm10.state_abbr Is Not Null                   AND
               hm10.property_type_name Is Not Null              AND    hm10.owner_occupancy_name Is Not Null         AND
               hm10.msamd_name Is Not Null                      AND    hm10.lien_status_name Is Not Null             AND
               hm10.hoepa_status_name Is Not Null               AND    hm10.co_applicant_sex_name Is Not Null        AND
               hm10.co_applicant_race_name_1 Is Not Null        AND    hm10.co_applicant_ethnicity_name Is Not Null  AND
               hm10.applicant_sex_name Is Not Null              AND    hm10.applicant_race_name_1 Is Not Null        AND
               hm10.applicant_ethnicity_name Is Not Null        AND    hm10.agency_abbr Is Not Null                  AND
               hm10.loan_amount_000s Is Not Null                AND    hm10.rate_spread Is Not Null                  AND
               hm10.number_of_1_to_4_family_units Is Not Null   AND    hm10.rate_spread != ''
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm_10_u_raw)
     ) ,

     hm_10_de AS --extract the raw data fields for segment of denied loans with loan type = Conventional
     ( SELECT hm10.action_taken_name As act, hm10.as_of_year, NULL as rate_spread,
              hm10.tract_to_msamd_income, hm10.population, hm10.agency_abbr, hm10.minority_population,
              hm10.number_of_owner_occupied_units, hm10.loan_amount_000s, hm10.number_of_1_to_4_family_units,
              hm10.hud_median_family_income, hm10.applicant_income_000s, hm10.state_abbr, hm10.property_type_name,
              hm10.owner_occupancy_name, hm10.msamd_name, hm10.lien_status_name, hm10.hoepa_status_name,
              hm10.co_applicant_sex_name, hm10.co_applicant_ethnicity_name, hm10.co_applicant_race_name_1,
              hm10.applicant_sex_name, hm10.applicant_race_name_1, hm10.applicant_ethnicity_name,
              --extract all denial reasons to concatenate in the following CTE
              hm10.denial_reason_name_1, hm10.denial_reason_name_2, hm10.denial_reason_name_3
       FROM usa_mortgage_market.hmda_lar_2010_allrecords hm10
       WHERE hm10.action_taken_name = 'Application denied by financial institution'
         AND hm10.loan_type_name = 'Conventional'
         AND ( hm10.denial_reason_name_1 Is Not Null Or denial_reason_name_2 Is not Null
                 OR denial_reason_name_3 Is Not Null ) --dropping all missing denied loans without a denial reason
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm10.as_of_year Is Not NULL                      AND    hm10.tract_to_msamd_income Is Not Null        AND
               hm10.population Is Not Null                      AND    hm10.minority_population Is Not Null          AND
               hm10.number_of_owner_occupied_units Is Not Null  AND    hm10.hud_median_family_income Is Not Null     AND
               hm10.applicant_income_000s Is Not Null           AND    hm10.state_abbr Is Not Null                   AND
               hm10.property_type_name Is Not Null              AND    hm10.owner_occupancy_name Is Not Null         AND
               hm10.msamd_name Is Not Null                      AND    hm10.lien_status_name Is Not Null             AND
               hm10.hoepa_status_name Is Not Null               AND    hm10.co_applicant_sex_name Is Not Null        AND
               hm10.co_applicant_race_name_1 Is Not Null        AND    hm10.co_applicant_ethnicity_name Is Not Null  AND
               hm10.applicant_sex_name Is Not Null              AND    hm10.applicant_race_name_1 Is Not Null        AND
               hm10.applicant_ethnicity_name Is Not Null        AND    hm10.agency_abbr Is Not Null                  AND
               hm10.loan_amount_000s Is Not Null                AND    hm10.number_of_1_to_4_family_units Is Not Null
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm10_u_raw)
     ) ,

     hm_10_u_raw AS --UNION ALL for the two segmented CTEs to get balanced dataset of 25K
     ( SELECT hm_a.* FROM(SELECT * FROM hm_10_ap ORDER BY random() LIMIT 12500) hm_a
            UNION ALL --note that we take another random sample of 125000 of each 25K randomized sample segment
       SELECT hm_d.* FROM(SELECT * FROM hm_10_de ORDER BY random() LIMIT 12500) hm_d
     ) ,

     hmda_2010_transform As
     ( SELECT
           --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm10.act = 'Loan originated' THEN 1 ELSE 0 END As act_outc, hm10.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( hm10.rate_spread As Varchar(5)) As NUMERIC ) As rate_spread, --NB: must be num bc of decimals
           --concatenating denial reasons into one feature on which we do engineering using python
           CONCAT_WS(', ', hm10.denial_reason_name_1, hm10.denial_reason_name_2, hm10.denial_reason_name_3) as denials,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm10.tract_to_msamd_income IS NULL THEN NULL ELSE hm10.tract_to_msamd_income END
                          As Varchar(5)) As NUMERIC )  As tract_to_msamd_inc, --NB: must be num bc of dec
           hm10.population As pop, ROUND(hm10.minority_population, 2) As minority_pop_perc,
           hm10.number_of_owner_occupied_units As num_owoc_units,
           hm10.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm10.loan_amount_000s As ln_amt_000s,
           hm10.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm10.applicant_income_000s IS NULL THEN NULL ELSE hm10.applicant_income_000s END
                          As Varchar(5)) As INT ) As applic_inc_000s,
           CAST(hm10.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm10.property_type_name As VARCHAR(108) ) As property_type_nm,
           CAST(hm10.owner_occupancy_name As VARCHAR(108)) As own_occ_nm,
           CAST(hm10.msamd_name As VARCHAR(108)) As msamd_nm,
           CAST(hm10.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm10.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm10.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm10.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm10.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm10.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm10.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm10.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm10.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM hm_10_u_raw hm10
       ORDER BY random()
     ) ,

    hmda_2010_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2010_transform WHERE act_outc = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2010_transform WHERE act_outc = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm10_u.*
  INTO interim_datasets_v2.hmda10_srandom_bal_25K
  FROM hmda_2010_union hm10_u
ORDER BY random()
;
/*--------------------------- end HMDA 2010 ---------------------------*/


--> END z_bz_AWS_paddleloancanoe <---





/* ==> NB: All subsequent HMDA individual years will apply the same SQL logic from the code above, but will be stripped
       of comments for length
*/





---> z_tn_AWS_paddleloancanoe <---


/*----------------------------------------------------- HMDA 2011 ----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets_v2.hmda11_srandom_bal_25K ;
--
WITH
     hm_11_ap AS --extract the raw data fields for segment of approved loans with loan type = Conventional
     ( SELECT hm11.action_taken_name As act, hm11.as_of_year, TRIM(LEADING '0' FROM rate_spread) As rate_spread,
              hm11.tract_to_msamd_income, hm11.population, hm11.agency_abbr, hm11.minority_population,
              hm11.number_of_owner_occupied_units, hm11.loan_amount_000s, hm11.number_of_1_to_4_family_units,
              hm11.hud_median_family_income, hm11.applicant_income_000s, hm11.state_abbr, hm11.property_type_name,
              hm11.owner_occupancy_name, hm11.msamd_name, hm11.lien_status_name, hm11.hoepa_status_name,
              hm11.co_applicant_sex_name, hm11.co_applicant_ethnicity_name, hm11.co_applicant_race_name_1,
              hm11.applicant_sex_name, hm11.applicant_race_name_1, hm11.applicant_ethnicity_name,
              --set Null because all denial reasons are null for approved loans
              NULL As denial_reason_name_1, NULL AS denial_reason_name_2, NULL AS denial_reason_name_3
       FROM public.hmda_lar_2011_allrecords hm11
       WHERE hm11.action_taken_name = 'Loan originated'
         AND hm11.loan_type_name = 'Conventional'
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm11.as_of_year Is Not NULL                      AND    hm11.tract_to_msamd_income Is Not Null        AND
               hm11.population Is Not Null                      AND    hm11.minority_population Is Not Null          AND
               hm11.number_of_owner_occupied_units Is Not Null  AND    hm11.hud_median_family_income Is Not Null     AND
               hm11.applicant_income_000s Is Not Null           AND    hm11.state_abbr Is Not Null                   AND
               hm11.property_type_name Is Not Null              AND    hm11.owner_occupancy_name Is Not Null         AND
               hm11.msamd_name Is Not Null                      AND    hm11.lien_status_name Is Not Null             AND
               hm11.hoepa_status_name Is Not Null               AND    hm11.co_applicant_sex_name Is Not Null        AND
               hm11.co_applicant_race_name_1 Is Not Null        AND    hm11.co_applicant_ethnicity_name Is Not Null  AND
               hm11.applicant_sex_name Is Not Null              AND    hm11.applicant_race_name_1 Is Not Null        AND
               hm11.applicant_ethnicity_name Is Not Null        AND    hm11.agency_abbr Is Not Null                  AND
               hm11.loan_amount_000s Is Not Null                AND    hm11.rate_spread Is Not Null                  AND
               hm11.number_of_1_to_4_family_units Is Not Null   AND    hm11.rate_spread != ''
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm_11_u_raw)
     ) ,

     hm_11_de AS --extract the raw data fields for segment of denied loans with loan type = Conventional
     ( SELECT hm11.action_taken_name As act, hm11.as_of_year, NULL as rate_spread,
              hm11.tract_to_msamd_income, hm11.population, hm11.agency_abbr, hm11.minority_population,
              hm11.number_of_owner_occupied_units, hm11.loan_amount_000s, hm11.number_of_1_to_4_family_units,
              hm11.hud_median_family_income, hm11.applicant_income_000s, hm11.state_abbr, hm11.property_type_name,
              hm11.owner_occupancy_name, hm11.msamd_name, hm11.lien_status_name, hm11.hoepa_status_name,
              hm11.co_applicant_sex_name, hm11.co_applicant_ethnicity_name, hm11.co_applicant_race_name_1,
              hm11.applicant_sex_name, hm11.applicant_race_name_1, hm11.applicant_ethnicity_name,
              --extract all denial reasons to concatenate in the following CTE
              hm11.denial_reason_name_1, hm11.denial_reason_name_2, hm11.denial_reason_name_3
       FROM public.hmda_lar_2011_allrecords hm11
       WHERE hm11.action_taken_name = 'Application denied by financial institution'
         AND hm11.loan_type_name = 'Conventional'
         AND ( hm11.denial_reason_name_1 Is Not Null Or denial_reason_name_2 Is not Null
                 OR denial_reason_name_3 Is Not Null ) --dropping all missing denied loans without a denial reason
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm11.as_of_year Is Not NULL                      AND    hm11.tract_to_msamd_income Is Not Null        AND
               hm11.population Is Not Null                      AND    hm11.minority_population Is Not Null          AND
               hm11.number_of_owner_occupied_units Is Not Null  AND    hm11.hud_median_family_income Is Not Null     AND
               hm11.applicant_income_000s Is Not Null           AND    hm11.state_abbr Is Not Null                   AND
               hm11.property_type_name Is Not Null              AND    hm11.owner_occupancy_name Is Not Null         AND
               hm11.msamd_name Is Not Null                      AND    hm11.lien_status_name Is Not Null             AND
               hm11.hoepa_status_name Is Not Null               AND    hm11.co_applicant_sex_name Is Not Null        AND
               hm11.co_applicant_race_name_1 Is Not Null        AND    hm11.co_applicant_ethnicity_name Is Not Null  AND
               hm11.applicant_sex_name Is Not Null              AND    hm11.applicant_race_name_1 Is Not Null        AND
               hm11.applicant_ethnicity_name Is Not Null        AND    hm11.agency_abbr Is Not Null                  AND
               hm11.loan_amount_000s Is Not Null                AND    hm11.number_of_1_to_4_family_units Is Not Null
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm11_u_raw)
     ) ,

     hm_11_u_raw AS --UNION ALL for the two segmented CTEs to get balanced dataset of 25K
     ( SELECT hm_a.* FROM(SELECT * FROM hm_11_ap ORDER BY random() LIMIT 12500) hm_a
            UNION ALL --note that we take another random sample of 125000 of each 25K randomized sample segment
       SELECT hm_d.* FROM(SELECT * FROM hm_11_de ORDER BY random() LIMIT 12500) hm_d
     ) ,

     hmda_2011_transform As
     ( SELECT
           --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm11.act = 'Loan originated' THEN 1 ELSE 0 END As act_outc, hm11.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( hm11.rate_spread As Varchar(5)) As NUMERIC ) As rate_spread, --NB: must be num bc of decimals
           --concatenating denial reasons into one feature on which we do engineering using python
           CONCAT_WS(', ', hm11.denial_reason_name_1, hm11.denial_reason_name_2, hm11.denial_reason_name_3) as denials,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm11.tract_to_msamd_income IS NULL THEN NULL ELSE hm11.tract_to_msamd_income END
                          As Varchar(5)) As NUMERIC )  As tract_to_msamd_inc, --NB: must be num bc of dec
           hm11.population As pop, ROUND(hm11.minority_population, 2) As minority_pop_perc,
           hm11.number_of_owner_occupied_units As num_owoc_units,
           hm11.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm11.loan_amount_000s As ln_amt_000s,
           hm11.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm11.applicant_income_000s IS NULL THEN NULL ELSE hm11.applicant_income_000s END
                          As Varchar(5)) As INT ) As applic_inc_000s,
           CAST(hm11.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm11.property_type_name As VARCHAR(118) ) As property_type_nm,
           CAST(hm11.owner_occupancy_name As VARCHAR(118)) As own_occ_nm,
           CAST(hm11.msamd_name As VARCHAR(118)) As msamd_nm,
           CAST(hm11.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm11.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm11.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm11.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm11.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm11.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm11.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm11.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm11.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM hm_11_u_raw hm11
       ORDER BY random()
     ) ,

    hmda_2011_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2011_transform WHERE act_outc = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2011_transform WHERE act_outc = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm11_u.*
  INTO interim_datasets_v2.hmda11_srandom_bal_25K
  FROM hmda_2011_union hm11_u
ORDER BY random()
;
/*--------------------------- end HMDA 2011 ---------------------------*/







/*----------------------------------------------------- HMDA 2012 ----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets_v2.hmda12_srandom_bal_25K ;
--
WITH
     hm_12_ap AS --extract the raw data fields for segment of approved loans with loan type = Conventional
     ( SELECT hm12.action_taken_name As act, hm12.as_of_year, TRIM(LEADING '0' FROM rate_spread) As rate_spread,
              hm12.tract_to_msamd_income, hm12.population, hm12.agency_abbr, hm12.minority_population,
              hm12.number_of_owner_occupied_units, hm12.loan_amount_000s, hm12.number_of_1_to_4_family_units,
              hm12.hud_median_family_income, hm12.applicant_income_000s, hm12.state_abbr, hm12.property_type_name,
              hm12.owner_occupancy_name, hm12.msamd_name, hm12.lien_status_name, hm12.hoepa_status_name,
              hm12.co_applicant_sex_name, hm12.co_applicant_ethnicity_name, hm12.co_applicant_race_name_1,
              hm12.applicant_sex_name, hm12.applicant_race_name_1, hm12.applicant_ethnicity_name,
              --set Null because all denial reasons are null for approved loans
              NULL As denial_reason_name_1, NULL AS denial_reason_name_2, NULL AS denial_reason_name_3
       FROM public.hmda_lar_2012_allrecords hm12
       WHERE hm12.action_taken_name = 'Loan originated'
         AND hm12.loan_type_name = 'Conventional'
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm12.as_of_year Is Not NULL                      AND    hm12.tract_to_msamd_income Is Not Null        AND
               hm12.population Is Not Null                      AND    hm12.minority_population Is Not Null          AND
               hm12.number_of_owner_occupied_units Is Not Null  AND    hm12.hud_median_family_income Is Not Null     AND
               hm12.applicant_income_000s Is Not Null           AND    hm12.state_abbr Is Not Null                   AND
               hm12.property_type_name Is Not Null              AND    hm12.owner_occupancy_name Is Not Null         AND
               hm12.msamd_name Is Not Null                      AND    hm12.lien_status_name Is Not Null             AND
               hm12.hoepa_status_name Is Not Null               AND    hm12.co_applicant_sex_name Is Not Null        AND
               hm12.co_applicant_race_name_1 Is Not Null        AND    hm12.co_applicant_ethnicity_name Is Not Null  AND
               hm12.applicant_sex_name Is Not Null              AND    hm12.applicant_race_name_1 Is Not Null        AND
               hm12.applicant_ethnicity_name Is Not Null        AND    hm12.agency_abbr Is Not Null                  AND
               hm12.loan_amount_000s Is Not Null                AND    hm12.rate_spread Is Not Null                  AND
               hm12.number_of_1_to_4_family_units Is Not Null   AND    hm12.rate_spread != ''
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm_12_u_raw)
     ) ,

     hm_12_de AS --extract the raw data fields for segment of denied loans with loan type = Conventional
     ( SELECT hm12.action_taken_name As act, hm12.as_of_year, NULL as rate_spread,
              hm12.tract_to_msamd_income, hm12.population, hm12.agency_abbr, hm12.minority_population,
              hm12.number_of_owner_occupied_units, hm12.loan_amount_000s, hm12.number_of_1_to_4_family_units,
              hm12.hud_median_family_income, hm12.applicant_income_000s, hm12.state_abbr, hm12.property_type_name,
              hm12.owner_occupancy_name, hm12.msamd_name, hm12.lien_status_name, hm12.hoepa_status_name,
              hm12.co_applicant_sex_name, hm12.co_applicant_ethnicity_name, hm12.co_applicant_race_name_1,
              hm12.applicant_sex_name, hm12.applicant_race_name_1, hm12.applicant_ethnicity_name,
              --extract all denial reasons to concatenate in the following CTE
              hm12.denial_reason_name_1, hm12.denial_reason_name_2, hm12.denial_reason_name_3
       FROM public.hmda_lar_2012_allrecords hm12
       WHERE hm12.action_taken_name = 'Application denied by financial institution'
         AND hm12.loan_type_name = 'Conventional'
         AND ( hm12.denial_reason_name_1 Is Not Null Or denial_reason_name_2 Is not Null
                 OR denial_reason_name_3 Is Not Null ) --dropping all missing denied loans without a denial reason
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm12.as_of_year Is Not NULL                      AND    hm12.tract_to_msamd_income Is Not Null        AND
               hm12.population Is Not Null                      AND    hm12.minority_population Is Not Null          AND
               hm12.number_of_owner_occupied_units Is Not Null  AND    hm12.hud_median_family_income Is Not Null     AND
               hm12.applicant_income_000s Is Not Null           AND    hm12.state_abbr Is Not Null                   AND
               hm12.property_type_name Is Not Null              AND    hm12.owner_occupancy_name Is Not Null         AND
               hm12.msamd_name Is Not Null                      AND    hm12.lien_status_name Is Not Null             AND
               hm12.hoepa_status_name Is Not Null               AND    hm12.co_applicant_sex_name Is Not Null        AND
               hm12.co_applicant_race_name_1 Is Not Null        AND    hm12.co_applicant_ethnicity_name Is Not Null  AND
               hm12.applicant_sex_name Is Not Null              AND    hm12.applicant_race_name_1 Is Not Null        AND
               hm12.applicant_ethnicity_name Is Not Null        AND    hm12.agency_abbr Is Not Null                  AND
               hm12.loan_amount_000s Is Not Null                AND    hm12.number_of_1_to_4_family_units Is Not Null
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm12_u_raw)
     ) ,

     hm_12_u_raw AS --UNION ALL for the two segmented CTEs to get balanced dataset of 25K
     ( SELECT hm_a.* FROM(SELECT * FROM hm_12_ap ORDER BY random() LIMIT 12500) hm_a
            UNION ALL --note that we take another random sample of 125000 of each 25K randomized sample segment
       SELECT hm_d.* FROM(SELECT * FROM hm_12_de ORDER BY random() LIMIT 12500) hm_d
     ) ,

     hmda_2012_transform As
     ( SELECT
           --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm12.act = 'Loan originated' THEN 1 ELSE 0 END As act_outc, hm12.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( hm12.rate_spread As Varchar(5)) As NUMERIC ) As rate_spread, --NB: must be num bc of decimals
           --concatenating denial reasons into one feature on which we do engineering using python
           CONCAT_WS(', ', hm12.denial_reason_name_1, hm12.denial_reason_name_2, hm12.denial_reason_name_3) as denials,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm12.tract_to_msamd_income IS NULL THEN NULL ELSE hm12.tract_to_msamd_income END
                          As Varchar(5)) As NUMERIC )  As tract_to_msamd_inc, --NB: must be num bc of dec
           hm12.population As pop, ROUND(hm12.minority_population, 2) As minority_pop_perc,
           hm12.number_of_owner_occupied_units As num_owoc_units,
           hm12.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm12.loan_amount_000s As ln_amt_000s,
           hm12.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm12.applicant_income_000s IS NULL THEN NULL ELSE hm12.applicant_income_000s END
                          As Varchar(5)) As INT ) As applic_inc_000s,
           CAST(hm12.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm12.property_type_name As VARCHAR(128) ) As property_type_nm,
           CAST(hm12.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm12.msamd_name As VARCHAR(128)) As msamd_nm,
           CAST(hm12.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm12.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm12.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm12.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm12.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm12.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm12.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm12.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM hm_12_u_raw hm12
       ORDER BY random()
     ) ,

    hmda_2012_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2012_transform WHERE act_outc = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2012_transform WHERE act_outc = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm12_u.*
  INTO interim_datasets_v2.hmda12_srandom_bal_25K
  FROM hmda_2012_union hm12_u
ORDER BY random()
;
/*--------------------------- end HMDA 2012 ---------------------------*/







/*----------------------------------------------------- HMDA 2013 ----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets_v2.hmda13_srandom_bal_25K ;
--
WITH
     hm_13_ap AS --extract the raw data fields for segment of approved loans with loan type = Conventional
     ( SELECT hm13.action_taken_name As act, hm13.as_of_year, TRIM(LEADING '0' FROM rate_spread) As rate_spread,
              hm13.tract_to_msamd_income, hm13.population, hm13.agency_abbr, hm13.minority_population,
              hm13.number_of_owner_occupied_units, hm13.loan_amount_000s, hm13.number_of_1_to_4_family_units,
              hm13.hud_median_family_income, hm13.applicant_income_000s, hm13.state_abbr, hm13.property_type_name,
              hm13.owner_occupancy_name, hm13.msamd_name, hm13.lien_status_name, hm13.hoepa_status_name,
              hm13.co_applicant_sex_name, hm13.co_applicant_ethnicity_name, hm13.co_applicant_race_name_1,
              hm13.applicant_sex_name, hm13.applicant_race_name_1, hm13.applicant_ethnicity_name,
              --set Null because all denial reasons are null for approved loans
              NULL As denial_reason_name_1, NULL AS denial_reason_name_2, NULL AS denial_reason_name_3
       FROM public.hmda_lar_2013_allrecords hm13
       WHERE hm13.action_taken_name = 'Loan originated'
         AND hm13.loan_type_name = 'Conventional'
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm13.as_of_year Is Not NULL                      AND    hm13.tract_to_msamd_income Is Not Null        AND
               hm13.population Is Not Null                      AND    hm13.minority_population Is Not Null          AND
               hm13.number_of_owner_occupied_units Is Not Null  AND    hm13.hud_median_family_income Is Not Null     AND
               hm13.applicant_income_000s Is Not Null           AND    hm13.state_abbr Is Not Null                   AND
               hm13.property_type_name Is Not Null              AND    hm13.owner_occupancy_name Is Not Null         AND
               hm13.msamd_name Is Not Null                      AND    hm13.lien_status_name Is Not Null             AND
               hm13.hoepa_status_name Is Not Null               AND    hm13.co_applicant_sex_name Is Not Null        AND
               hm13.co_applicant_race_name_1 Is Not Null        AND    hm13.co_applicant_ethnicity_name Is Not Null  AND
               hm13.applicant_sex_name Is Not Null              AND    hm13.applicant_race_name_1 Is Not Null        AND
               hm13.applicant_ethnicity_name Is Not Null        AND    hm13.agency_abbr Is Not Null                  AND
               hm13.loan_amount_000s Is Not Null                AND    hm13.rate_spread Is Not Null                  AND
               hm13.number_of_1_to_4_family_units Is Not Null   AND    hm13.rate_spread != ''
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm_13_u_raw)
     ) ,

     hm_13_de AS --extract the raw data fields for segment of denied loans with loan type = Conventional
     ( SELECT hm13.action_taken_name As act, hm13.as_of_year, NULL as rate_spread,
              hm13.tract_to_msamd_income, hm13.population, hm13.agency_abbr, hm13.minority_population,
              hm13.number_of_owner_occupied_units, hm13.loan_amount_000s, hm13.number_of_1_to_4_family_units,
              hm13.hud_median_family_income, hm13.applicant_income_000s, hm13.state_abbr, hm13.property_type_name,
              hm13.owner_occupancy_name, hm13.msamd_name, hm13.lien_status_name, hm13.hoepa_status_name,
              hm13.co_applicant_sex_name, hm13.co_applicant_ethnicity_name, hm13.co_applicant_race_name_1,
              hm13.applicant_sex_name, hm13.applicant_race_name_1, hm13.applicant_ethnicity_name,
              --extract all denial reasons to concatenate in the following CTE
              hm13.denial_reason_name_1, hm13.denial_reason_name_2, hm13.denial_reason_name_3
       FROM public.hmda_lar_2013_allrecords hm13
       WHERE hm13.action_taken_name = 'Application denied by financial institution'
         AND hm13.loan_type_name = 'Conventional'
         AND ( hm13.denial_reason_name_1 Is Not Null Or denial_reason_name_2 Is not Null
                 OR denial_reason_name_3 Is Not Null ) --dropping all missing denied loans without a denial reason
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm13.as_of_year Is Not NULL                      AND    hm13.tract_to_msamd_income Is Not Null        AND
               hm13.population Is Not Null                      AND    hm13.minority_population Is Not Null          AND
               hm13.number_of_owner_occupied_units Is Not Null  AND    hm13.hud_median_family_income Is Not Null     AND
               hm13.applicant_income_000s Is Not Null           AND    hm13.state_abbr Is Not Null                   AND
               hm13.property_type_name Is Not Null              AND    hm13.owner_occupancy_name Is Not Null         AND
               hm13.msamd_name Is Not Null                      AND    hm13.lien_status_name Is Not Null             AND
               hm13.hoepa_status_name Is Not Null               AND    hm13.co_applicant_sex_name Is Not Null        AND
               hm13.co_applicant_race_name_1 Is Not Null        AND    hm13.co_applicant_ethnicity_name Is Not Null  AND
               hm13.applicant_sex_name Is Not Null              AND    hm13.applicant_race_name_1 Is Not Null        AND
               hm13.applicant_ethnicity_name Is Not Null        AND    hm13.agency_abbr Is Not Null                  AND
               hm13.loan_amount_000s Is Not Null                AND    hm13.number_of_1_to_4_family_units Is Not Null
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm13_u_raw)
     ) ,

     hm_13_u_raw AS --UNION ALL for the two segmented CTEs to get balanced dataset of 25K
     ( SELECT hm_a.* FROM(SELECT * FROM hm_13_ap ORDER BY random() LIMIT 12500) hm_a
            UNION ALL --note that we take another random sample of 125000 of each 25K randomized sample segment
       SELECT hm_d.* FROM(SELECT * FROM hm_13_de ORDER BY random() LIMIT 12500) hm_d
     ) ,

     hmda_2013_transform As
     ( SELECT
           --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm13.act = 'Loan originated' THEN 1 ELSE 0 END As act_outc, hm13.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( hm13.rate_spread As Varchar(5)) As NUMERIC ) As rate_spread, --NB: must be num bc of decimals
           --concatenating denial reasons into one feature on which we do engineering using python
           CONCAT_WS(', ', hm13.denial_reason_name_1, hm13.denial_reason_name_2, hm13.denial_reason_name_3) as denials,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm13.tract_to_msamd_income IS NULL THEN NULL ELSE hm13.tract_to_msamd_income END
                          As Varchar(5)) As NUMERIC )  As tract_to_msamd_inc, --NB: must be num bc of dec
           hm13.population As pop, ROUND(hm13.minority_population, 2) As minority_pop_perc,
           hm13.number_of_owner_occupied_units As num_owoc_units,
           hm13.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm13.loan_amount_000s As ln_amt_000s,
           hm13.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm13.applicant_income_000s IS NULL THEN NULL ELSE hm13.applicant_income_000s END
                          As Varchar(5)) As INT ) As applic_inc_000s,
           CAST(hm13.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm13.property_type_name As VARCHAR(128) ) As property_type_nm,
           CAST(hm13.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm13.msamd_name As VARCHAR(128)) As msamd_nm,
           CAST(hm13.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm13.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm13.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm13.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm13.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm13.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm13.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm13.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm13.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM hm_13_u_raw hm13
       ORDER BY random()
     ) ,

    hmda_2013_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2013_transform WHERE act_outc = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2013_transform WHERE act_outc = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm13_u.*
  INTO interim_datasets_v2.hmda13_srandom_bal_25K
  FROM hmda_2013_union hm13_u
ORDER BY random()
;
/*--------------------------- end HMDA 2013 ---------------------------*/






        /*---------------------------------- Union 2014-2015 ----------------------------------*/
                ;
                WITH
                   hmda_union_2011_to_2016 AS
                   (
                     SELECT hm11.* FROM interim_datasets_v2.hmda11_srandom_bal_25k hm11
                        UNION ALL
                     SELECT hm12.* FROM interim_datasets_v2.hmda12_srandom_bal_25K hm12
                        UNION ALL
                     SELECT hm13.* FROM interim_datasets_v2.hmda13_srandom_bal_25K hm13
                   )
                SELECT hm_u.*
                  INTO interim_datasets_v2.hmda_2014_2015_union_srandom_bal_50k
                  FROM hmda_union_2011_to_2016 hm_u
                ;
        /*-------------------------------------------------------------------------------------*/


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
DROP TABLE IF EXISTS interim_datasets_v2.hmda14_srandom_bal_25K ;
--
WITH
     hm_14_ap AS --extract the raw data fields for segment of approved loans with loan type = Conventional
     ( SELECT hm14.action_taken_name As act, hm14.as_of_year, TRIM(LEADING '0' FROM rate_spread) As rate_spread,
              hm14.tract_to_msamd_income, hm14.population, hm14.agency_abbr, hm14.minority_population,
              hm14.number_of_owner_occupied_units, hm14.loan_amount_000s, hm14.number_of_1_to_4_family_units,
              hm14.hud_median_family_income, hm14.applicant_income_000s, hm14.state_abbr, hm14.property_type_name,
              hm14.owner_occupancy_name, hm14.msamd_name, hm14.lien_status_name, hm14.hoepa_status_name,
              hm14.co_applicant_sex_name, hm14.co_applicant_ethnicity_name, hm14.co_applicant_race_name_1,
              hm14.applicant_sex_name, hm14.applicant_race_name_1, hm14.applicant_ethnicity_name,
              --set Null because all denial reasons are null for approved loans
              NULL As denial_reason_name_1, NULL AS denial_reason_name_2, NULL AS denial_reason_name_3
       FROM public.hmda_lar_2014_allrecords hm14
       WHERE hm14.action_taken_name = 'Loan originated'
         AND hm14.loan_type_name = 'Conventional'
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm14.as_of_year Is Not NULL                      AND    hm14.tract_to_msamd_income Is Not Null        AND
               hm14.population Is Not Null                      AND    hm14.minority_population Is Not Null          AND
               hm14.number_of_owner_occupied_units Is Not Null  AND    hm14.hud_median_family_income Is Not Null     AND
               hm14.applicant_income_000s Is Not Null           AND    hm14.state_abbr Is Not Null                   AND
               hm14.property_type_name Is Not Null              AND    hm14.owner_occupancy_name Is Not Null         AND
               hm14.msamd_name Is Not Null                      AND    hm14.lien_status_name Is Not Null             AND
               hm14.hoepa_status_name Is Not Null               AND    hm14.co_applicant_sex_name Is Not Null        AND
               hm14.co_applicant_race_name_1 Is Not Null        AND    hm14.co_applicant_ethnicity_name Is Not Null  AND
               hm14.applicant_sex_name Is Not Null              AND    hm14.applicant_race_name_1 Is Not Null        AND
               hm14.applicant_ethnicity_name Is Not Null        AND    hm14.agency_abbr Is Not Null                  AND
               hm14.loan_amount_000s Is Not Null                AND    hm14.rate_spread Is Not Null                  AND
               hm14.number_of_1_to_4_family_units Is Not Null   AND    hm14.rate_spread != ''                        AND
               hm14.applicant_income_000s != ''

             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm_14_u_raw)
     ) ,

     hm_14_de AS --extract the raw data fields for segment of denied loans with loan type = Conventional
     ( SELECT hm14.action_taken_name As act, hm14.as_of_year, NULL as rate_spread,
              hm14.tract_to_msamd_income, hm14.population, hm14.agency_abbr, hm14.minority_population,
              hm14.number_of_owner_occupied_units, hm14.loan_amount_000s, hm14.number_of_1_to_4_family_units,
              hm14.hud_median_family_income, hm14.applicant_income_000s, hm14.state_abbr, hm14.property_type_name,
              hm14.owner_occupancy_name, hm14.msamd_name, hm14.lien_status_name, hm14.hoepa_status_name,
              hm14.co_applicant_sex_name, hm14.co_applicant_ethnicity_name, hm14.co_applicant_race_name_1,
              hm14.applicant_sex_name, hm14.applicant_race_name_1, hm14.applicant_ethnicity_name,
              --extract all denial reasons to concatenate in the following CTE
              hm14.denial_reason_name_1, hm14.denial_reason_name_2, hm14.denial_reason_name_3
       FROM public.hmda_lar_2014_allrecords hm14
       WHERE hm14.action_taken_name = 'Application denied by financial institution'
         AND hm14.loan_type_name = 'Conventional'
         AND ( hm14.denial_reason_name_1 Is Not Null Or denial_reason_name_2 Is not Null
                 OR denial_reason_name_3 Is Not Null ) --dropping all missing denied loans without a denial reason
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm14.as_of_year Is Not NULL                      AND    hm14.tract_to_msamd_income Is Not Null        AND
               hm14.population Is Not Null                      AND    hm14.minority_population Is Not Null          AND
               hm14.number_of_owner_occupied_units Is Not Null  AND    hm14.hud_median_family_income Is Not Null     AND
               hm14.applicant_income_000s Is Not Null           AND    hm14.state_abbr Is Not Null                   AND
               hm14.property_type_name Is Not Null              AND    hm14.owner_occupancy_name Is Not Null         AND
               hm14.msamd_name Is Not Null                      AND    hm14.lien_status_name Is Not Null             AND
               hm14.hoepa_status_name Is Not Null               AND    hm14.co_applicant_sex_name Is Not Null        AND
               hm14.co_applicant_race_name_1 Is Not Null        AND    hm14.co_applicant_ethnicity_name Is Not Null  AND
               hm14.applicant_sex_name Is Not Null              AND    hm14.applicant_race_name_1 Is Not Null        AND
               hm14.applicant_ethnicity_name Is Not Null        AND    hm14.agency_abbr Is Not Null                  AND
               hm14.loan_amount_000s Is Not Null                AND    hm14.applicant_income_000s != ''              AND
               hm14.number_of_1_to_4_family_units Is Not Null
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm14_u_raw)
     ) ,

     hm_14_u_raw AS --UNION ALL for the two segmented CTEs to get balanced dataset of 25K
     ( SELECT hm_a.* FROM(SELECT * FROM hm_14_ap ORDER BY random() LIMIT 12500) hm_a
            UNION ALL --note that we take another random sample of 125000 of each 25K randomized sample segment
       SELECT hm_d.* FROM(SELECT * FROM hm_14_de ORDER BY random() LIMIT 12500) hm_d
     ) ,

     hmda_2014_transform As
     ( SELECT
           --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm14.act = 'Loan originated' THEN 1 ELSE 0 END As act_outc, hm14.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( hm14.rate_spread As Varchar(5)) As NUMERIC ) As rate_spread, --NB: must be num bc of decimals
           --concatenating denial reasons into one feature on which we do engineering using python
           CONCAT_WS(', ', hm14.denial_reason_name_1, hm14.denial_reason_name_2, hm14.denial_reason_name_3) as denials,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm14.tract_to_msamd_income IS NULL THEN NULL ELSE hm14.tract_to_msamd_income END
                          As Varchar(5)) As NUMERIC )  As tract_to_msamd_inc, --NB: must be num bc of dec
           hm14.population As pop, ROUND(hm14.minority_population, 2) As minority_pop_perc,
           hm14.number_of_owner_occupied_units As num_owoc_units,
           hm14.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm14.loan_amount_000s As ln_amt_000s,
           hm14.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm14.applicant_income_000s = '' THEN NULL ELSE hm14.applicant_income_000s END
                          As Varchar(5)) As INT ) As applic_inc_000s,
           CAST(hm14.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm14.property_type_name As VARCHAR(128) ) As property_type_nm,
           CAST(hm14.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm14.msamd_name As VARCHAR(128)) As msamd_nm,
           CAST(hm14.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm14.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm14.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm14.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm14.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm14.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm14.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm14.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm14.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM hm_14_u_raw hm14
       ORDER BY random()
     ) ,
    hmda_2014_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2014_transform WHERE act_outc = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2014_transform WHERE act_outc = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm14_u.*
  INTO interim_datasets_v2.hmda14_srandom_bal_25K
  FROM hmda_2014_union hm14_u
ORDER BY random()
;
/*--------------------------- end HMDA 2014 ---------------------------*/




/*----------------------------------------------------- HMDA 2015 ----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets_v2.hmda15_srandom_bal_25K ;
--
WITH
     hm_15_ap AS --extract the raw data fields for segment of approved loans with loan type = Conventional
     ( SELECT hm15.action_taken_name As act, hm15.as_of_year, TRIM(LEADING '0' FROM rate_spread) As rate_spread,
              hm15.tract_to_msamd_income, hm15.population, hm15.agency_abbr, hm15.minority_population,
              hm15.number_of_owner_occupied_units, hm15.loan_amount_000s, hm15.number_of_1_to_4_family_units,
              hm15.hud_median_family_income, hm15.applicant_income_000s, hm15.state_abbr, hm15.property_type_name,
              hm15.owner_occupancy_name, hm15.msamd_name, hm15.lien_status_name, hm15.hoepa_status_name,
              hm15.co_applicant_sex_name, hm15.co_applicant_ethnicity_name, hm15.co_applicant_race_name_1,
              hm15.applicant_sex_name, hm15.applicant_race_name_1, hm15.applicant_ethnicity_name,
              --set Null because all denial reasons are null for approved loans
              NULL As denial_reason_name_1, NULL AS denial_reason_name_2, NULL AS denial_reason_name_3
       FROM usa_mortgage_market.hmda_lar_2015_allrecords hm15
       WHERE hm15.action_taken_name = 'Loan originated'
         AND hm15.loan_type_name = 'Conventional'
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm15.as_of_year Is Not NULL                      AND    hm15.tract_to_msamd_income Is Not Null        AND
               hm15.population Is Not Null                      AND    hm15.minority_population Is Not Null          AND
               hm15.number_of_owner_occupied_units Is Not Null  AND    hm15.hud_median_family_income Is Not Null     AND
               hm15.applicant_income_000s Is Not Null           AND    hm15.state_abbr Is Not Null                   AND
               hm15.property_type_name Is Not Null              AND    hm15.owner_occupancy_name Is Not Null         AND
               hm15.msamd_name Is Not Null                      AND    hm15.lien_status_name Is Not Null             AND
               hm15.hoepa_status_name Is Not Null               AND    hm15.co_applicant_sex_name Is Not Null        AND
               hm15.co_applicant_race_name_1 Is Not Null        AND    hm15.co_applicant_ethnicity_name Is Not Null  AND
               hm15.applicant_sex_name Is Not Null              AND    hm15.applicant_race_name_1 Is Not Null        AND
               hm15.applicant_ethnicity_name Is Not Null        AND    hm15.agency_abbr Is Not Null                  AND
               hm15.loan_amount_000s Is Not Null                AND    hm15.rate_spread Is Not Null                  AND
               hm15.number_of_1_to_4_family_units Is Not Null   AND    hm15.rate_spread != ''
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm_15_u_raw)
     ) ,

     hm_15_de AS --extract the raw data fields for segment of denied loans with loan type = Conventional
     ( SELECT hm15.action_taken_name As act, hm15.as_of_year, NULL as rate_spread,
              hm15.tract_to_msamd_income, hm15.population, hm15.agency_abbr, hm15.minority_population,
              hm15.number_of_owner_occupied_units, hm15.loan_amount_000s, hm15.number_of_1_to_4_family_units,
              hm15.hud_median_family_income, hm15.applicant_income_000s, hm15.state_abbr, hm15.property_type_name,
              hm15.owner_occupancy_name, hm15.msamd_name, hm15.lien_status_name, hm15.hoepa_status_name,
              hm15.co_applicant_sex_name, hm15.co_applicant_ethnicity_name, hm15.co_applicant_race_name_1,
              hm15.applicant_sex_name, hm15.applicant_race_name_1, hm15.applicant_ethnicity_name,
              --extract all denial reasons to concatenate in the following CTE
              hm15.denial_reason_name_1, hm15.denial_reason_name_2, hm15.denial_reason_name_3
       FROM usa_mortgage_market.hmda_lar_2015_allrecords hm15
       WHERE hm15.action_taken_name = 'Application denied by financial institution'
         AND hm15.loan_type_name = 'Conventional'
         AND ( hm15.denial_reason_name_1 Is Not Null Or denial_reason_name_2 Is not Null
                 OR denial_reason_name_3 Is Not Null ) --dropping all missing denied loans without a denial reason
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm15.as_of_year Is Not NULL                      AND    hm15.tract_to_msamd_income Is Not Null        AND
               hm15.population Is Not Null                      AND    hm15.minority_population Is Not Null          AND
               hm15.number_of_owner_occupied_units Is Not Null  AND    hm15.hud_median_family_income Is Not Null     AND
               hm15.applicant_income_000s Is Not Null           AND    hm15.state_abbr Is Not Null                   AND
               hm15.property_type_name Is Not Null              AND    hm15.owner_occupancy_name Is Not Null         AND
               hm15.msamd_name Is Not Null                      AND    hm15.lien_status_name Is Not Null             AND
               hm15.hoepa_status_name Is Not Null               AND    hm15.co_applicant_sex_name Is Not Null        AND
               hm15.co_applicant_race_name_1 Is Not Null        AND    hm15.co_applicant_ethnicity_name Is Not Null  AND
               hm15.applicant_sex_name Is Not Null              AND    hm15.applicant_race_name_1 Is Not Null        AND
               hm15.applicant_ethnicity_name Is Not Null        AND    hm15.agency_abbr Is Not Null                  AND
               hm15.number_of_1_to_4_family_units Is Not Null   AND    hm15.loan_amount_000s Is Not Null

             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm15_u_raw)
     ) ,

     hm_15_u_raw AS --UNION ALL for the two segmented CTEs to get balanced dataset of 25K
     ( SELECT hm_a.* FROM(SELECT * FROM hm_15_ap ORDER BY random() LIMIT 12500) hm_a
            UNION ALL --note that we take another random sample of 125000 of each 25K randomized sample segment
       SELECT hm_d.* FROM(SELECT * FROM hm_15_de ORDER BY random() LIMIT 12500) hm_d
     ) ,

     hmda_2015_transform As
     ( SELECT
           --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm15.act = 'Loan originated' THEN 1 ELSE 0 END As act_outc, hm15.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( hm15.rate_spread As Varchar(5)) As NUMERIC ) As rate_spread, --NB: must be num bc of decimals
           --concatenating denial reasons into one feature on which we do engineering using python
           CONCAT_WS(', ', hm15.denial_reason_name_1, hm15.denial_reason_name_2, hm15.denial_reason_name_3) as denials,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm15.tract_to_msamd_income IS NULL THEN NULL ELSE hm15.tract_to_msamd_income END
                          As Varchar(5)) As NUMERIC )  As tract_to_msamd_inc, --NB: must be num bc of dec
           hm15.population As pop, ROUND(hm15.minority_population, 2) As minority_pop_perc,
           hm15.number_of_owner_occupied_units As num_owoc_units,
           hm15.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm15.loan_amount_000s As ln_amt_000s,
           hm15.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm15.applicant_income_000s IS NULL THEN NULL ELSE hm15.applicant_income_000s END
                          As Varchar(5)) As INT ) As applic_inc_000s,
           CAST(hm15.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm15.property_type_name As VARCHAR(128) ) As property_type_nm,
           CAST(hm15.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm15.msamd_name As VARCHAR(128)) As msamd_nm,
           CAST(hm15.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm15.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm15.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm15.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm15.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm15.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm15.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm15.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm15.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM hm_15_u_raw hm15
       ORDER BY random()
     ) ,

    hmda_2015_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2015_transform WHERE act_outc = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2015_transform WHERE act_outc = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm15_u.*
  INTO interim_datasets_v2.hmda15_srandom_bal_25K
  FROM hmda_2015_union hm15_u
ORDER BY random()
;
/*--------------------------- end HMDA 2015 ---------------------------*/








        /*---------------------------------- Union 2014-2015 ----------------------------------*/
                ;
                WITH
                   hmda_union_2014_2015 AS
                   (
                     SELECT hm14.* FROM interim_datasets_v2.hmda14_srandom_bal_25k hm14
                        UNION ALL
                     SELECT hm15.* FROM interim_datasets_v2.hmda15_srandom_bal_25K hm15
                   )
                SELECT hm_u.*
                  INTO interim_datasets_v2.hmda_2014_2015_union_srandom_bal_50k
                  FROM hmda_union_2014_2015 hm_u
                ;
        /*-------------------------------------------------------------------------------------*/


--> END z_bz_AWS_paddleloancanoe <---





--> z_ak_AWS_paddleloancanoe <---

/*----------------------------------------------------- HMDA 2016 ----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets_v2.hmda16_srandom_bal_25K ;
--
WITH
     hm_16_ap AS --extract the raw data fields for segment of approved loans with loan type = Conventional
     ( SELECT hm16.action_taken_name As act, hm16.as_of_year, TRIM(LEADING '0' FROM rate_spread) As rate_spread,
              hm16.tract_to_msamd_income, hm16.population, hm16.agency_abbr, hm16.minority_population,
              hm16.number_of_owner_occupied_units, hm16.loan_amount_000s, hm16.number_of_1_to_4_family_units,
              hm16.hud_median_family_income, hm16.applicant_income_000s, hm16.state_abbr, hm16.property_type_name,
              hm16.owner_occupancy_name, hm16.msamd_name, hm16.lien_status_name, hm16.hoepa_status_name,
              hm16.co_applicant_sex_name, hm16.co_applicant_ethnicity_name, hm16.co_applicant_race_name_1,
              hm16.applicant_sex_name, hm16.applicant_race_name_1, hm16.applicant_ethnicity_name,
              --set Null because all denial reasons are null for approved loans
              NULL As denial_reason_name_1, NULL AS denial_reason_name_2, NULL AS denial_reason_name_3
       FROM public.hmda_lar_2016_allrecords hm16
       WHERE hm16.action_taken_name = 'Loan originated'
         AND hm16.loan_type_name = 'Conventional'
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm16.as_of_year Is Not NULL                      AND    hm16.tract_to_msamd_income Is Not Null        AND
               hm16.population Is Not Null                      AND    hm16.minority_population Is Not Null          AND
               hm16.number_of_owner_occupied_units Is Not Null  AND    hm16.hud_median_family_income Is Not Null     AND
               hm16.applicant_income_000s Is Not Null           AND    hm16.state_abbr Is Not Null                   AND
               hm16.property_type_name Is Not Null              AND    hm16.owner_occupancy_name Is Not Null         AND
               hm16.msamd_name Is Not Null                      AND    hm16.lien_status_name Is Not Null             AND
               hm16.hoepa_status_name Is Not Null               AND    hm16.co_applicant_sex_name Is Not Null        AND
               hm16.co_applicant_race_name_1 Is Not Null        AND    hm16.co_applicant_ethnicity_name Is Not Null  AND
               hm16.applicant_sex_name Is Not Null              AND    hm16.applicant_race_name_1 Is Not Null        AND
               hm16.applicant_ethnicity_name Is Not Null        AND    hm16.agency_abbr Is Not Null                  AND
               hm16.loan_amount_000s Is Not Null                AND    hm16.rate_spread Is Not Null                  AND
               hm16.number_of_1_to_4_family_units Is Not Null   AND    hm16.rate_spread != ''                        AND
               hm16.applicant_income_000s != ''

             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm_16_u_raw)
     ) ,

     hm_16_de AS --extract the raw data fields for segment of denied loans with loan type = Conventional
     ( SELECT hm16.action_taken_name As act, hm16.as_of_year, NULL as rate_spread,
              hm16.tract_to_msamd_income, hm16.population, hm16.agency_abbr, hm16.minority_population,
              hm16.number_of_owner_occupied_units, hm16.loan_amount_000s, hm16.number_of_1_to_4_family_units,
              hm16.hud_median_family_income, hm16.applicant_income_000s, hm16.state_abbr, hm16.property_type_name,
              hm16.owner_occupancy_name, hm16.msamd_name, hm16.lien_status_name, hm16.hoepa_status_name,
              hm16.co_applicant_sex_name, hm16.co_applicant_ethnicity_name, hm16.co_applicant_race_name_1,
              hm16.applicant_sex_name, hm16.applicant_race_name_1, hm16.applicant_ethnicity_name,
              --extract all denial reasons to concatenate in the following CTE
              hm16.denial_reason_name_1, hm16.denial_reason_name_2, hm16.denial_reason_name_3
       FROM public.hmda_lar_2016_allrecords hm16
       WHERE hm16.action_taken_name = 'Application denied by financial institution'
         AND hm16.loan_type_name = 'Conventional'
         AND ( hm16.denial_reason_name_1 Is Not Null Or denial_reason_name_2 Is not Null
                 OR denial_reason_name_3 Is Not Null ) --dropping all missing denied loans without a denial reason
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm16.as_of_year Is Not NULL                      AND    hm16.tract_to_msamd_income Is Not Null        AND
               hm16.population Is Not Null                      AND    hm16.minority_population Is Not Null          AND
               hm16.number_of_owner_occupied_units Is Not Null  AND    hm16.hud_median_family_income Is Not Null     AND
               hm16.applicant_income_000s Is Not Null           AND    hm16.state_abbr Is Not Null                   AND
               hm16.property_type_name Is Not Null              AND    hm16.owner_occupancy_name Is Not Null         AND
               hm16.msamd_name Is Not Null                      AND    hm16.lien_status_name Is Not Null             AND
               hm16.hoepa_status_name Is Not Null               AND    hm16.co_applicant_sex_name Is Not Null        AND
               hm16.co_applicant_race_name_1 Is Not Null        AND    hm16.co_applicant_ethnicity_name Is Not Null  AND
               hm16.applicant_sex_name Is Not Null              AND    hm16.applicant_race_name_1 Is Not Null        AND
               hm16.applicant_ethnicity_name Is Not Null        AND    hm16.agency_abbr Is Not Null                  AND
               hm16.loan_amount_000s Is Not Null                AND    hm16.applicant_income_000s != ''              AND
               hm16.number_of_1_to_4_family_units Is Not Null
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm16_u_raw)
     ) ,

     hm_16_u_raw AS --UNION ALL for the two segmented CTEs to get balanced dataset of 25K
     ( SELECT hm_a.* FROM(SELECT * FROM hm_16_ap ORDER BY random() LIMIT 12500) hm_a
            UNION ALL --note that we take another random sample of 125000 of each 25K randomized sample segment
       SELECT hm_d.* FROM(SELECT * FROM hm_16_de ORDER BY random() LIMIT 12500) hm_d
     ) ,

     hmda_2016_transform As
     ( SELECT
           --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm16.act = 'Loan originated' THEN 1 ELSE 0 END As act_outc, hm16.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( hm16.rate_spread As Varchar(5)) As NUMERIC ) As rate_spread, --NB: must be num bc of decimals
           --concatenating denial reasons into one feature on which we do engineering using python
           CONCAT_WS(', ', hm16.denial_reason_name_1, hm16.denial_reason_name_2, hm16.denial_reason_name_3) as denials,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm16.tract_to_msamd_income IS NULL THEN NULL ELSE hm16.tract_to_msamd_income END
                          As Varchar(5)) As NUMERIC )  As tract_to_msamd_inc, --NB: must be num bc of dec
           hm16.population As pop, ROUND(hm16.minority_population, 2) As minority_pop_perc,
           hm16.number_of_owner_occupied_units As num_owoc_units,
           hm16.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm16.loan_amount_000s As ln_amt_000s,
           hm16.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm16.applicant_income_000s = '' THEN NULL ELSE hm16.applicant_income_000s END
                          As Varchar(5)) As INT ) As applic_inc_000s,
           CAST(hm16.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm16.property_type_name As VARCHAR(128) ) As property_type_nm,
           CAST(hm16.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm16.msamd_name As VARCHAR(128)) As msamd_nm,
           CAST(hm16.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm16.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm16.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm16.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm16.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm16.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm16.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm16.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm16.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM hm_16_u_raw hm16
       ORDER BY random()
     ) ,

    hmda_2016_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2016_transform WHERE act_outc = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2016_transform WHERE act_outc = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm16_u.*
  INTO interim_datasets_v2.hmda16_srandom_bal_25K
  FROM hmda_2016_union hm16_u
ORDER BY random()
;
/*--------------------------- end HMDA 2016 ---------------------------*/






/*----------------------------------------------------- HMDA 2017 ----------------------------------------------------*/
DROP TABLE IF EXISTS interim_datasets_v2.hmda17_srandom_bal_25K ;
--
WITH
     hm_17_ap AS --extract the raw data fields for segment of approved loans with loan type = Conventional
     ( SELECT hm17.action_taken_name As act, hm17.as_of_year, TRIM(LEADING '0' FROM rate_spread) As rate_spread,
              hm17.tract_to_msamd_income, hm17.population, hm17.agency_abbr, hm17.minority_population,
              hm17.number_of_owner_occupied_units, hm17.loan_amount_000s, hm17.number_of_1_to_4_family_units,
              hm17.hud_median_family_income, hm17.applicant_income_000s, hm17.state_abbr, hm17.property_type_name,
              hm17.owner_occupancy_name, hm17.msamd_name, hm17.lien_status_name, hm17.hoepa_status_name,
              hm17.co_applicant_sex_name, hm17.co_applicant_ethnicity_name, hm17.co_applicant_race_name_1,
              hm17.applicant_sex_name, hm17.applicant_race_name_1, hm17.applicant_ethnicity_name,
              --set Null because all denial reasons are null for approved loans
              NULL As denial_reason_name_1, NULL AS denial_reason_name_2, NULL AS denial_reason_name_3
       FROM public.hmda_lar_2017_allrecords hm17
       WHERE hm17.action_taken_name = 'Loan originated'
         AND hm17.loan_type_name = 'Conventional'
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm17.as_of_year Is Not NULL                      AND    hm17.tract_to_msamd_income Is Not Null        AND
               hm17.population Is Not Null                      AND    hm17.minority_population Is Not Null          AND
               hm17.number_of_owner_occupied_units Is Not Null  AND    hm17.hud_median_family_income Is Not Null     AND
               hm17.applicant_income_000s Is Not Null           AND    hm17.state_abbr Is Not Null                   AND
               hm17.property_type_name Is Not Null              AND    hm17.owner_occupancy_name Is Not Null         AND
               hm17.msamd_name Is Not Null                      AND    hm17.lien_status_name Is Not Null             AND
               hm17.hoepa_status_name Is Not Null               AND    hm17.co_applicant_sex_name Is Not Null        AND
               hm17.co_applicant_race_name_1 Is Not Null        AND    hm17.co_applicant_ethnicity_name Is Not Null  AND
               hm17.applicant_sex_name Is Not Null              AND    hm17.applicant_race_name_1 Is Not Null        AND
               hm17.applicant_ethnicity_name Is Not Null        AND    hm17.agency_abbr Is Not Null                  AND
               hm17.loan_amount_000s Is Not Null                AND    hm17.rate_spread Is Not Null                  AND
               hm17.number_of_1_to_4_family_units Is Not Null   AND    hm17.rate_spread != ''                        AND
               hm17.applicant_income_000s != ''

             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm_17_u_raw)
     ) ,

     hm_17_de AS --extract the raw data fields for segment of denied loans with loan type = Conventional
     ( SELECT hm17.action_taken_name As act, hm17.as_of_year, NULL as rate_spread,
              hm17.tract_to_msamd_income, hm17.population, hm17.agency_abbr, hm17.minority_population,
              hm17.number_of_owner_occupied_units, hm17.loan_amount_000s, hm17.number_of_1_to_4_family_units,
              hm17.hud_median_family_income, hm17.applicant_income_000s, hm17.state_abbr, hm17.property_type_name,
              hm17.owner_occupancy_name, hm17.msamd_name, hm17.lien_status_name, hm17.hoepa_status_name,
              hm17.co_applicant_sex_name, hm17.co_applicant_ethnicity_name, hm17.co_applicant_race_name_1,
              hm17.applicant_sex_name, hm17.applicant_race_name_1, hm17.applicant_ethnicity_name,
              --extract all denial reasons to concatenate in the following CTE
              hm17.denial_reason_name_1, hm17.denial_reason_name_2, hm17.denial_reason_name_3
       FROM public.hmda_lar_2017_allrecords hm17
       WHERE hm17.action_taken_name = 'Application denied by financial institution'
         AND hm17.loan_type_name = 'Conventional'
         AND ( hm17.denial_reason_name_1 Is Not Null Or denial_reason_name_2 Is not Null
                 OR denial_reason_name_3 Is Not Null ) --dropping all missing denied loans without a denial reason
         AND ( --dropping all missing values from our dataset bc they are not missing in any systemic way
               hm17.as_of_year Is Not NULL                      AND    hm17.tract_to_msamd_income Is Not Null        AND
               hm17.population Is Not Null                      AND    hm17.minority_population Is Not Null          AND
               hm17.number_of_owner_occupied_units Is Not Null  AND    hm17.hud_median_family_income Is Not Null     AND
               hm17.applicant_income_000s Is Not Null           AND    hm17.state_abbr Is Not Null                   AND
               hm17.property_type_name Is Not Null              AND    hm17.owner_occupancy_name Is Not Null         AND
               hm17.msamd_name Is Not Null                      AND    hm17.lien_status_name Is Not Null             AND
               hm17.hoepa_status_name Is Not Null               AND    hm17.co_applicant_sex_name Is Not Null        AND
               hm17.co_applicant_race_name_1 Is Not Null        AND    hm17.co_applicant_ethnicity_name Is Not Null  AND
               hm17.applicant_sex_name Is Not Null              AND    hm17.applicant_race_name_1 Is Not Null        AND
               hm17.applicant_ethnicity_name Is Not Null        AND    hm17.agency_abbr Is Not Null                  AND
               hm17.loan_amount_000s Is Not Null                AND    hm17.applicant_income_000s != ''              AND
               hm17.number_of_1_to_4_family_units Is Not Null
             )
       ORDER BY random()
       LIMIT 25000 --random sample of 25000, which we then use to take another random sample of 12500 (CTE: hm17_u_raw)
     ) ,

     hm_17_u_raw AS --UNION ALL for the two segmented CTEs to get balanced dataset of 25K
     ( SELECT hm_a.* FROM(SELECT * FROM hm_17_ap ORDER BY random() LIMIT 12500) hm_a
            UNION ALL --note that we take another random sample of 125000 of each 25K randomized sample segment
       SELECT hm_d.* FROM(SELECT * FROM hm_17_de ORDER BY random() LIMIT 12500) hm_d
     ) ,

     hmda_2017_transform As
     ( SELECT
           --simple binary assignment of 1 or 0 , bc the WHERE clause in this CTE segments the approved/denied subset
           CASE WHEN hm17.act = 'Loan originated' THEN 1 ELSE 0 END As act_outc, hm17.as_of_year As action_year,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( hm17.rate_spread As Varchar(5)) As NUMERIC ) As rate_spread, --NB: must be num bc of decimals
           --concatenating denial reasons into one feature on which we do engineering using python
           CONCAT_WS(', ', hm17.denial_reason_name_1, hm17.denial_reason_name_2, hm17.denial_reason_name_3) as denials,
           --must use embedded functions to typecast this as integer because of NULL/space characters in raw data
           CAST( CAST( CASE WHEN hm17.tract_to_msamd_income IS NULL THEN NULL ELSE hm17.tract_to_msamd_income END
                          As Varchar(5)) As NUMERIC )  As tract_to_msamd_inc, --NB: must be num bc of dec
           hm17.population As pop, ROUND(hm17.minority_population, 2) As minority_pop_perc,
           hm17.number_of_owner_occupied_units As num_owoc_units,
           hm17.number_of_1_to_4_family_units As num_1to4_fam_units,
           hm17.loan_amount_000s As ln_amt_000s,
           hm17.hud_median_family_income As hud_med_fm_inc,
           --must use embedded functions all-in-one to typecast this as integer because raw data stores it as TEXT
           CAST( CAST( CASE WHEN hm17.applicant_income_000s = '' THEN NULL ELSE hm17.applicant_income_000s END
                          As Varchar(5)) As INT ) As applic_inc_000s,
           CAST(hm17.state_abbr As VARCHAR(5)) As state_abbr,
           CAST(hm17.property_type_name As VARCHAR(128) ) As property_type_nm,
           CAST(hm17.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
           CAST(hm17.msamd_name As VARCHAR(128)) As msamd_nm,
           CAST(hm17.lien_status_name As VARCHAR(56)) As lien_status_nm,
           CAST(hm17.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
           CAST(hm17.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
           CAST(hm17.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
           CAST(hm17.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
           CAST(hm17.applicant_sex_name As VARCHAR(28)) As applic_sex,
           CAST(hm17.applicant_race_name_1 As VARCHAR(28)) As applic_race,
           CAST(hm17.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
           CAST(hm17.agency_abbr As VARCHAR(28)) As agency_abbr
       FROM hm_17_u_raw hm17
       ORDER BY random()
     ) ,

    hmda_2017_union AS
    ( SELECT hm_a.* FROM(SELECT * FROM hmda_2017_transform WHERE act_outc = 1 ORDER BY random() LIMIT 12500) hm_a
            UNION ALL
      SELECT hm_a.* FROM(SELECT * FROM hmda_2017_transform WHERE act_outc = 0 ORDER BY random() LIMIT 12500) hm_a
    )

SELECT hm17_u.*
  INTO interim_datasets_v2.hmda17_srandom_bal_25K
  FROM hmda_2017_union hm17_u
ORDER BY random()
;
/*--------------------------- end HMDA 2017 ---------------------------*/





        /*---------------------------------- Union 2016-2017 ----------------------------------*/
                ;
                WITH
                   hmda_union_2016_2017 AS
                   (
                     SELECT hm16.* FROM interim_datasets_v2.hmda16_srandom_bal_25k hm16
                        UNION ALL
                     SELECT hm17.* FROM interim_datasets_v2.hmda17_srandom_bal_25K hm17
                   )
                SELECT hm_u.*
                  INTO interim_datasets_v2.hmda_2016_2017_union_srandom_bal_50k
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
