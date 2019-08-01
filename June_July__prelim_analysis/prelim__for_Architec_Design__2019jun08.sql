/*** =========================================> SATURDAY, JUNE 08, 2019 <========================================= ***/


--> A. First, create PostgresSQL database in Amazon for Capstone: Paddle Your Loan Canoe; connect RDS
--> B. Second, upload dataset (main one is the mortgage data Anne compiled; other applicable ones  with potential
--> C. Update Architecture/Design documentation to included the use case for I & II above.



/*** --- A. store prelim mungling in a "testing" schema & store "fallback" kaggle data in another schema --- ***/
CREATE SCHEMA z__testing_mungling ;
CREATE SCHEMA y__kaggle_fallback_ln_data ;
/*** --- end A.---- ***/
---;



/*** --------- B. Prelim Mungling Query (grouped) -- 2017 Only: Cast a few key Columns & Store in a VIEW  --------- ***/

CREATE VIEW z__testing_mungling."00_test_mortg_w_pop_educ__2017" AS

SELECT
       --> a. main: casting a few key MORTGAGE data fields:
       CAST(us17.county_name As varchar(18))                               As county_nm,
       CAST(us17.agency_name As varchar(128))                              As agency_nm,
       CAST(us17.loan_type_name As varchar(128))                           As loan_type_nm,
       CAST(us17.loan_purpose_name As varchar(128))                        As loan_purpose_nm,
       CAST(us17.action_taken_name As varchar(128))                        As action_taken_nm,
       us17.applicant_income_000s                                          As applicant_income_000s,
       us17.hud_median_family_income                                       As hud_median_fam_inc,
       us17.loan_amount_000s                                               As loan_amt_000s,
            --three embedded fuctions all in one below: assigns hierarchy in CASE, and converts to numeric in two steps
       CAST ( CAST ( CASE
                         WHEN us17.rate_spread = '' Then '0'
                         ELSE us17.rate_spread
                     END As varchar(5)
                   ) As numeric
             )
       As rt_spread,
       --*
       --> b. macro-econ: casting and joining a few key EDUCATION data fields:
       CAST(educ17."Perc_adults w_less than a HS diploma_2013-17" As int)  As prc_blw_HS_dipl__2013_17_5yrAvg,
       CAST(educ17."Perc_adults w_ HS diploma only_2013-17" As int)        As prc_HS_dipl__2013_17_5yrAvg,
       CAST(educ17."Perc_adults w_BA deg or higher_2013-17" As int)        As prc_BA_deg_or_higher__2013_17_5yrAvg,
       --*
       --> c. macro-econ: casting and joining a few key POPULATION data fields:
       CAST(pop17.r_birth_2017 AS INT)                                     As r_birth_2017,
       CAST(pop17.r_international_mig_2017 AS INT)                         As r_intnl_mig_2017,
       CAST(pop17.r_natural_inc_2017 AS INT)                               As r_natural_inc_2017
       --*

    FROM usa_mortgage_market.hmda_lar__2017 us17
    LEFT OUTER JOIN v__macro_economic_indicators.education__acs_1970_to_2017_5yravgs educ17
        ON us17.county_name = educ17."Area name"
    LEFT OUTER JOIN v__macro_economic_indicators.populationestimates__usda_ers_2010_to_2018 pop17
        ON us17.county_name = pop17.area_name

    --> c. Condition for all county rows: granular filtering for loans a. that are $500K+(much higher than nat avg
    WHERE us17.loan_amount_000s >= 500
       OR ( CAST ( CAST ( CASE
                              WHEN us17.rate_spread = '' Then '0'
                              ELSE us17.rate_spread
                           END As varchar(5)
                   ) As numeric
                 ) >= 5
          )
;

/*** ----- END B.  B. Prelim Mungling Query (grouped) -- 2017 Only: Cast a few key Columns & Store in a VIEW  -----***/



/*** ----------------------- tests: short queries for testing listed below  --------------------- ***/
--
Select us17.county_name, us17.agency_name,
       us17.hud_median_family_income, pop17.r_birth_2017,
       pop17.r_international_mig_2017, pop17.r_natural_inc_2017,
       educ17."Less than a HS diploma_2013-17", educ17.state

    From paddle_loan_canoe.usa_mortgage_market.hmda_lar__2017 us17
    Left Outer Join v__macro_economic_indicators.populationestimates__usda_ers_2010_to_2018 pop17
        On us17.county_name = pop17.area_name
    Left Outer Join v__macro_economic_indicators.education__acs_1970_to_2017_5yravgs educ17
        On educ17."Area name" = us17.county_name
;
--
Select Count(Distinct us16.county_name)        As dist__counties_cts,
       Count(Distinct us16.action_taken_name)  As dist__acts_taken_cts,
       Count(Distinct us16.loan_type_name)     As dist__ln_type_cts,
       Count(Distinct us16.loan_purpose_name)  As dist__ln_purp_cts
    From usa_mortgage_market.hmda_lar_2016 us16
;
--
select count(*) as ct from usa_mortgage_market.hmda_lar__2017 where rate_spread = '' group by rate_spread;
--
select count(*) as ct from usa_mortgage_market.hmda_lar__2017 where rate_spread Is Null group by rate_spread;
--
select distinct us17.loan_type_name, us17.loan_purpose_name

    from usa_mortgage_market.hmda_lar__2017 us17 order by loan_type_name
;
--
/*** --------------------- end tests: short queries for testing listed below  ------------------- ***/
/*** =======================================> END SATURDAY, JUNE 08, 2019 <======================================= ***/




/*** =========================================> SATURDAY, JULY 15, 2019 <========================================= ***/

--> A. Finish uploading the rest of "main" dataset into schema "usa_mortgage_market";
--> B.1 Write a script that keeps only the variables we want (i.e. lables and codes);
--> B.2 Cast these variables and combne into one time series dataset.





/*** =======================================> END SATURDAY, JULY 08, 2019 <======================================= ***/










/*** =========================================> DAY, MONTH DD, YYYY <========================================= ***/

/*** =======================================> END DAY, MONTH DD, YYYY <======================================= ***/