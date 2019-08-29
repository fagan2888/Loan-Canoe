WITH table_cross_section_sample AS

( SELECT
                              --> a. main: casting a few key MORTGAGE data fields:
                                   CAST(us17.action_taken_name As varchar(56)) As outcome, CAST(nullif(as_of_year, loan_amount_000s) As INT) As year,
                                   CAST(denial_reason_name_1 As varchar(56)) dn_reason1 , CAST(us17.agency_name As varchar(56)) As agency,
                                   CAST(us17.state_name As varchar(28)) As state,         CAST(us17.county_name As varchar(56)) As county,
                                   CAST(us17.loan_type_name As varchar(56)) As ln_type,   CAST(us17.loan_purpose_name As varchar(56)) As ln_purp,
                                   us17.loan_amount_000s As ln_amt_000s, us17.hud_median_family_income As hud_med_fm_inc, population as pop,

                                       --two embedded fuctions and one CASE below: assigns hierarchy in CASE, and converts to num in two steps
                                   CAST ( CAST ( CASE
                                                     WHEN us17.rate_spread = '' Then '0'
                                                     ELSE us17.rate_spread
                                                 END As varchar(5)
                                               ) As numeric
                                        )
                                   As rt_spread,
                                       --categorize loan application outcome into two buckets: "Approved", "Denied, Not Accepted, or Withdrawn"
                                   CASE
                                       WHEN us17.action_taken_name In ('Loan originated', 'Loan purchased by the institution')
                                           THEN 'Approved or Loan Purchased by the Institution'
                                       ELSE 'Denied, Not Accepted, or Withdrawn'
                                   END outcome_bucket,
                              --*
                              --> b. macro-econ: casting and joining a few key EDUCATION data fields:
                                   CAST(educ17."Perc_adults w_less than a HS diploma_2013-17" As int)  As prc_blw_HS__2013_17_5yrAvg,
                                   CAST(educ17."Perc_adults w_ HS diploma only_2013-17" As int)        As prc_HS__2013_17_5yrAvg,
                                   CAST(educ17."Perc_adults w_BA deg or higher_2013-17" As int)        As prc_BA_plus__2013_17_5yrAvg,
                              --*
                              --> c. macro-econ: casting and joining a few key POPULATION data fields:
                                   CAST(pop17.r_birth_2017 AS INT)                                     As r_birth_2017,
                                   CAST(pop17.r_international_mig_2017 AS INT)                         As r_intl_mig_2017,
                                   CAST(pop17.r_natural_inc_2017 AS INT)                               As r_natural_inc_2017
                              --*
                           FROM usa_mortgage_market.hmda_lar__2017 us17
                           LEFT OUTER JOIN v__macro_economic_indicators.education__acs_1970_to_2017_5yravgs educ17
                                   ON us17.county_name = educ17."Area name"
                           LEFT OUTER JOIN v__macro_economic_indicators.populationestimates__usda_ers_2010_to_2018 pop17
                                   ON us17.county_name = pop17.area_name
                           LIMIT 50000
)
/*SELECT *
    FROM table_cross_section_sample
    WHERE prc_blw_HS__2013_17_5yrAvg = 11
        AND prc_HS__2013_17_5yrAvg = 27
        AND prc_BA_plus__2013_17_5yrAvg = 37
; */

--
Select *
    From table_cross_section_sample
    Where prc_HS__2013_17_5yrAvg IS NULL
        /*Is Null OR prc_blw_HS__2013_17_5yrAvg IS Null OR prc_BA_plus__2013_17_5yrAvg IS NULL*/
;


/*-------*/

WITH count_r_vars AS

    ( SELECT

       CAST(us17.action_taken_name As varchar(56)) As outcome, us17.as_of_year As year,
       CAST(denial_reason_name_1 As varchar(56)) dn_reason1 , CAST(us17.agency_name As varchar(56)) As agency,
       CAST(us17.state_name As varchar(28)) As state,         CAST(us17.county_name As varchar(56)) As county,
       CAST(educ17."Perc_adults w_less than a HS diploma_2013-17" As int)  As prc_blw_HS__2013_17_5yrAvg,
       CAST(educ17."Perc_adults w_ HS diploma only_2013-17" As int)        As prc_HS__2013_17_5yrAvg,
       CAST(educ17."Perc_adults w_BA deg or higher_2013-17" As int)        As prc_BA_plus__2013_17_5yrAvg,
       CAST(pop17.r_birth_2017 AS INT)                                     As r_birth_2017,
       CAST(pop17.r_international_mig_2017 AS INT)                         As r_intl_mig_2017,
       CAST(pop17.r_natural_inc_2017 AS INT)                               As r_natural_inc_2017


       FROM usa_mortgage_market.hmda_lar__2017 us17
       LEFT OUTER JOIN v__macro_economic_indicators.education__acs_1970_to_2017_5yravgs educ17
               ON us17.county_name = educ17."Area name"
       LEFT OUTER JOIN v__macro_economic_indicators.populationestimates__usda_ers_2010_to_2018 pop17
               ON us17.county_name = pop17.area_name
       LIMIT 50000
    )

SELECT 'r_birth_2017' As r_var__nm, COUNT(*) As null_counts FROM count_r_vars WHERE r_birth_2017 IS NULL
    UNION ALL
SELECT 'r_intl_mig_2017' As r_var__nm, COUNT(*) As null_counts  FROM count_r_vars WHERE r_birth_2017 IS NULL
    UNION ALL
SELECT 'r_nat_inc_2017' As r_var_nm, COUNT(*) As null_counts FROM count_r_vars WHERE r_natural_inc_2017 IS NULL

/************ /