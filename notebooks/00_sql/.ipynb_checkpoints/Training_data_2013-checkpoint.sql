SELECT
                              --> a. main: casting a few key MORTGAGE data fields:
                                   CAST(us13.action_taken_name As varchar(56)) As outcome, us13.as_of_year As year,
                                   CAST(denial_reason_name_1 As varchar(56)) dn_reason1 , CAST(us13.agency_name As varchar(56)) As agency,
                                   CAST(us13.state_name As varchar(28)) As state,         CAST(us13.county_name As varchar(56)) As county,
                                   CAST(us13.loan_type_name As varchar(56)) As ln_type,   CAST(us13.loan_purpose_name As varchar(56)) As ln_purp,
                                   us13.loan_amount_000s As ln_amt_000s, us13.hud_median_family_income As hud_med_fm_inc, population as pop,

                                       --two embedded fuctions and one CASE below: assigns hierarchy in CASE, and converts to num in two steps
                                   CAST ( CAST ( CASE
                                                     WHEN us13.rate_spread = '' Then '0'
                                                     ELSE us13.rate_spread
                                                 END As varchar(5)
                                               ) As numeric
                                        )
                                   As rt_spread,
                                       --categorize loan application outcome into two buckets: "Approved", "Denied, Not Accepted, or Withdrawn"
                                   CASE
                                       WHEN us13.action_taken_name In ('Loan originated', 'Loan purchased by the institution')
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
                                   CAST(pop13.r_birth_2013 AS INT)                                     As r_birth_2013,
                                   CAST(pop13.r_international_mig_2013 AS INT)                         As r_intl_mig_2013,
                                   CAST(pop13.r_natural_inc_2013 AS INT)                               As r_natural_inc_2013
                              --*
                           FROM usa_mortgage_market.hmda_lar_2013 us13
                           LEFT OUTER JOIN v__macro_economic_indicators.education__acs_1970_to_2017_5yravgs educ17
                                   ON us13.county_name = educ17."Area name"
                           LEFT OUTER JOIN v__macro_economic_indicators.populationestimates__usda_ers_2010_to_2018 pop13
                                   ON us13.county_name = pop13.area_name
                           LIMIT 50000