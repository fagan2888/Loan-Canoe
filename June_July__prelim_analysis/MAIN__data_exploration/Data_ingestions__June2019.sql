
/*** =============================> Data Ingestion Created:  SATURDAY, JULY 15, 2019 <============================= ***/

--> A. Finish uploading the rest of "main" dataset into schema "usa_mortgage_market";
--> B.1 Write a script that keeps only the variables we want (i.e. lables and codes);
--> B.2 Cast these variables and combine into one time series dataset.



/*** --------------------- I. Store codes with labels in a "code_keys" schema [ 2010 - 2017 ] --------------------- ***/

--> new schema
CREATE SCHEMA usa_mortgage_code_keys ;
--*

--> codes: action_taken
CREATE TABLE
    usa_mortgage_code_keys.acts_codes
    ( code int PRIMARY KEY,
      name varchar(60) NOT NULL
    )
;
INSERT INTO usa_mortgage_code_keys.acts_codes (code, name)
VALUES (1, 'Loan originated'),
       (2, 'Application approved but not accepted'),
       (3, 'Application denied by financial institution'),
       (4, 'Application withdrawn by applicant'),
       (5, 'File closed for incompleteness'),
       (6, 'Loan purchased by the institution'),
       (7, 'Preapproval request denied by financial institution'),
       (8, 'Preapproval request approved but not accepted')
;
--*

--> codes: action_taken
CREATE TABLE
    usa_mortgage_code_keys.agency_codes
    ( code int PRIMARY KEY,
      name varchar(60) NOT NULL,
      abbr varchar(5) NOT NULL
    )
;
INSERT INTO usa_mortgage_code_keys.agency_codes (code, name, abbr )
VALUES (1, 'Office of the Comptroller of the Currency', '0CC'),
       (2, 'Federal Reserve System', 'FRS'),
       (3, 'Federal Deposit Insurance Corporation', 'FDIC'),
       (5, 'National Credit Union Administration', 'NCUA'),
       (7, 'Department of Housing and Urban Development', 'HUD'),
       (9, 'Consumer Financial Protection Bureau', 'CFPB')
;
--*

select distinct us15.agency_code, us15.agency_abbr, us15.agency_name from usa_mortgage_market.hmda_lar_2015 us15;
/*** ------------------- END I. Store codes with labels in a "code_keys" schema [ 2010 - 2017 ] ------------------- ***/

--
INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)
VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');
      --
      SELECT Distinct m11.action_taken As code, CAST(m11.action_taken_name As varchar(60)) As nm, 'action_taken' As cat
      FROM usa_mortgage_market.hmda_lar_2011 m11
    ) select * from action_taken_codes order by cat, code Asc
    --*
SELECT Distinct act_code, action_taken_nm
    INTO usa_mortgage_code_keys.hdma_act_taken_codes
    FROM action_taken_codes
;
--

/*** --- END A. Store codes with labels in a "code_keys" schema --- ***/
---;

/*** ------------- II. Re-structuring: Assess and Execute Variable Changes -- Casting and UNION ALLs  ------------- ***/

SELECT us07.action_taken As act_taken, us07.
    FROM usa_mortgage_market.hmda_lar_2007 us07

Select * From paddle_loan_canoe.usa_mortgage_market.hmda_lar__2017 go;
--
SELECT CAST(us17.action_taken_name As varchar(56)) As outcome, us17.as_of_year As year,
       CAST(denial_reason_name_1 As varchar(56)) dn_reason1 , CAST(us17.agency_name As varchar(56)) As agency,
       CAST(us17.state_name As varchar(28)) As state,         CAST(us17.county_name As varchar(56)) As county,
       CAST(us17.loan_type_name As varchar(56)) As ln_type,   CAST(us17.loan_purpose_name As varchar(56)) As ln_purp, us17.loan_amount_000s As ln_amt_000s,
       us17.hud_median_family_income As hud_med_fm_inc, population as pop,
       CAST ( CAST ( CASE
                         WHEN us17.rate_spread = '' Then '0'
                         ELSE us17.rate_spread
                      END As varchar(5)
                   ) As numeric
             )
       As rt_spread
From usa_mortgage_market.hmda_lar__2017 us17
;
select distinct action_taken_name from usa_mortgage_market.hmda_lar__2017 ;
select distinct loan_type_name, property_type_name from usa_mortgage_market.hmda_lar__2017 ;
--

/*-----------------https://aws.amazon.com/blogs/database/managing-postgresql-users-and-roles/------------------*/
CREATE ROLE reporting_user WITH LOGIN PASSWORD 'team_loan_canoe2019' ;
--*
GRANT CONNECT ON DATABASE paddle_loan_canoe TO reporting_user;
GRANT USAGE ON SCHEMA usa_mortgage_market   TO reporting_user;
GRANT SELECT ON TABLE usa_mortgage_market.hmda_lar__2017 TO reporting_user;
--*
GRANT USAGE ON SCHEMA v__macro_economic_indicators TO reporting_user;
GRANT SELECT ON TABLE v__macro_economic_indicators.populationestimates__usda_ers_2010_to_2018 TO reporting_user;
GRANT SELECT ON TABLE v__macro_economic_indicators.education__acs_1970_to_2017_5yravgs        TO reporting_user;
/*------ End ------*/


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

/*** ----------- END II. Re-structuring: Assess and Execute Variable Changes -- Casting and UNION ALLs  ------------ ***/

/* ----- AGENDA ----- */
/*
   I.    Amazon RDS & Connecting to R
   II.   Datagrip
   III.  Queries with Sample Datasets
   IV.   Next Steps

 */




/*--- Query group A: SELECT * And Amazon tests ---*/

SELECT sp1.*, MAX()
  FROM "00_sample_data".sp500_sample1_2019may17 sp1
;
--
SELECT *
  FROM "00_sample_data".sp500_sample1_2019may17 sp1
  WHERE sp1.ticker LIKE '%AMZN%'
;
--
SELECT sp2.*
  FROM "00_sample_data".sp500_sample2_2019may17 sp2
;
--


CREATE VIEW "george".sp500_prices_changes AS

  SELECT spc.company, spc.sector, spc.ticker,
         sp1.assets, sp1.liabilities, sp1.azs, sp1.debt,
         (sp1.assets - sp1.liabilities)/(sp1.shares) As nav_simple,

         CASE
             WHEN (sp1.assets - sp1.liabilities)/(sp1.shares) > 15 THEN 'greatest'
             WHEN (sp1.assets - sp1.liabilities)/(sp1.shares) > 10 THEN 'great'
             WHEN (sp1.assets - sp1.liabilities)/(sp1.shares) >= 5 THEN 'good'
         END pr_to_nav_category,

         --below is nav as a percentage of price
         sp1.price/((sp1.assets - sp1.liabilities)/(sp1.shares)) As price_perc_nav

    FROM "00_sample_data".sp500_sector spc
    LEFT OUTER JOIN "00_sample_data".sp500_sample1_2019may17 sp1
        ON spc.ticker = LEFT(sp1.ticker, 4)

    WHERE sp1.azs > 2
      AND (spc.sector IN ('Consumer Discretionary', 'Information Technology')
        OR spc.sector LIKE '%Tech%' OR spc.sector like '%Fin%')

    GROUP BY spc.company, spc.sector, spc.ticker, sp1.assets,
             sp1.liabilities, sp1.azs, sp1.debt, sp1.shares, sp1.price,
            CASE
             WHEN (sp1.assets - sp1.liabilities)/(sp1.shares) > 15 THEN 'greatest'
             WHEN (sp1.assets - sp1.liabilities)/(sp1.shares) > 10 THEN 'great'
             WHEN (sp1.assets - sp1.liabilities)/(sp1.shares) >= 5 THEN 'good'
         END

    HAVING (sp1.assets - sp1.liabilities)/(sp1.shares) >= 5
;

create schema george;
/*--- END Query group A: Amazon tests ---*/



/* ----- AGENDA DEMO 2 ----- */
/*
   I.    Review: Amazon RDS
   II.   Connect to R
   III.  Live Queries
   IV.   Next Steps

 */





/*--- Query group Demo 2.A: "" ---*/

--Historical Prices S&P500
SELECT * FROM "00_sample_data".sp500_1 ;
--End Historical Prices S&P500

/*--- END Query group Demo 2.A: "" ---*/



/*--- Query group Demo 2.B: "" ---*/

--Select key data (by topic/theme) and append it
SELECT m17.ticker, m17.company, m17."EV.EBITDA", m17."Debt.Capital",
       m17."Debt.EBITDA", m17."Debt.Capital.rank", m17."Debt.EBITDA.rank",
       '2019-05-17' As date
  INTO "00_sample_data".ev_debt_select_ratios__2019may17
  FROM "00_sample_data".sp500_sample1_2019may17 m17

  WHERE company In ('MICRON TECHNOLOGY INC', 'PRINCIPAL FINANCIAL GROUP', 'FOOT LOCKER INC')
;
--

WITH union_append AS
 (

  SELECT m17.ticker, m17.company, m17."EV.EBITDA", m17."Debt.Capital",
         m17."Debt.EBITDA", m17."Debt.Capital.rank", m17."Debt.EBITDA.rank",
         '2019-05-17' As date, 'sample data' As notes
    FROM "00_sample_data".sp500_sample1_2019may17 m17

    WHERE company In ('MICRON TECHNOLOGY INC', 'PRINCIPAL FINANCIAL GROUP', 'FOOT LOCKER INC')


  UNION ALL

  SELECT m17.ticker, m17.company, m17."EV.EBITDA", m17."Debt.Capital",
         m17."Debt.EBITDA", m17."Debt.Capital.rank", m17."Debt.EBITDA.rank",
         '2019-05-18' As date, 'sample data' As notes
    FROM "00_sample_data".sp500_sample1_2019may17 m17

    WHERE company In ('MICRON TECHNOLOGY INC', 'PRINCIPAL FINANCIAL GROUP', 'FOOT LOCKER INC')
)
SELECT * INTO george.ev_debt__selected_ratios__TimeSeries_test
  FROM union_append
;
--End

/*--- END Query group Demo 2.B: "" ---*/







/*------------------------------------------> A. 3D array stored in a SQL table --------------------------------------*/

CREATE SCHEMA james_scratch ;

CREATE TABLE james_scratch.sp500_historical_prices_v2
    ( -- SERIAL creates sequence object, adds the NOT NULL, constraint and auto-increments
      ID SERIAL,
      ticker varchar(4) NOT NULL,
      date   timestamp NOT NULL,
      price  int NOT NULL,
      PRIMARY KEY (ticker, date, ID)
    )
;
SELECT Ticker, EXTRACT(YEAR from date) as yYar, AVG(price) as Avg_price
    FROM james_scratch.sp500_historical_prices_v2
    GROUP BY Ticker, EXTRACT(YEAR from date)
;
/*--- End ---*/


/* -- Live Workshop Demo: Ingesting Data, and Wrangling for Python/R readiness -- */

DROP TABLE "01_raw_imports"."S&P500-07-10-2019-raw_date";
--
Select '07-10-2019' As Date, sp710.*
    INTO "01_raw_imports"."S&P500-07-10-2019-raw_date"
    From "01_raw_imports"."S&P500-07-10-2019-raw" sp710
;
Select '07-12-2019' As Date, sp712.*
    INTO "01_raw_imports"."S&P500-07-12-2019-raw_date"
    From "01_raw_imports"."S&P500-07-12-2019-raw" sp712
;
--
DROP TABLE james_scratch.sp500_bbg_time_series;
--
CREATE TABLE james_scratch.sp500_bbg_time_series As

    ( -- a. July 12, 2019 raw file wrangled
          Select
            CAST(sp710.date As varchar(28)) As date__c,
            CAST(sp710.name As varchar(128)) As stock_name, CAST(sp710.ticker As varchar(5)) As stock_ticker,
            sp710."Last Px", sp710."EV/EBITDA T12M", sp710."LTD/Capital LF", sp710."Tot Debt to CFO"
          From "01_raw_imports"."S&P500-07-10-2019-raw_date" sp710

          UNION ALL

          Select
            CAST(sp712.date As varchar(28)) As date__c,
            CAST(sp712.name As varchar(128)) As stock_name, CAST(sp712.ticker As varchar(5)) As stock_ticker,
            sp712."Last Px", sp712."EV/EBITDA T12M", sp712."LTD/Capital LF", sp712."Tot Debt to CFO"
          From "01_raw_imports"."S&P500-07-12-2019-raw_date" sp712

    )
;
--*
ALTER TABLE james_scratch.sp500_bbg_time_series
   ADD CONSTRAINT PK_ID PRIMARY KEY (date__c, stock_name, stock_ticker);
--*
SELECT date__c, /*stock_ticker, stock_name,*/
       AVG("Last Px") As avg_px, AVG("LTD/Capital LF") As avg_ltd_capt_lf,
       AVG("Tot Debt to CFO") As avg_tot_debt_to_cfo
    FROM james_scratch.sp500_bbg_time_series
    GROUP BY date__c /*stock_ticker, stock_name*/
;
--*
SELECT date__c, stock_ticker, stock_name,
       "Last Px", "LTD/Capital LF", "Tot Debt to CFO"
    FROM james_scratch.sp500_bbg_time_series
    WHERE lower(stock_name) LIKE '%apple%'
;



/*** =========================================> Mon, JULY 22, 2019 <========================================= ***/

/*** =======================================> END DAY, MONTH DD, YYYY <======================================= ***/