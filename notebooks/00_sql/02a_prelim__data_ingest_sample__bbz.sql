
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
