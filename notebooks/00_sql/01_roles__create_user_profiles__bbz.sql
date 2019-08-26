
/******** This script creates ROLES and USER profile settings that allow us to read/write across AWS databases ********/


/* Note: This is core and critcal to our work,
         particularly in scaling our model selection to all HMDA loan data years (2009 - 2019) */



CREATE ROLE reporting_user WITH LOGIN PASSWORD 'team_loan_canoe2019' ;
--*

GRANT CONNECT ON DATABASE paddle_loan_canoe TO reporting_user;
GRANT USAGE ON SCHEMA usa_mortgage_market   TO reporting_user;
GRANT SELECT ON ALL TABLES IN SCHEMA usa_mortgage_market TO reporting_user;
GRANT SELECT ON ALL TABLES IN SCHEMA v__macro_economic_indicators TO reporting_user;
--*

GRANT USAGE ON SCHEMA aa__testing TO reporting_user;
    --https://tableplus.io/blog/2018/04/postgresql-how-to-create-read-only-user.html
    ALTER DEFAULT PRIVILEGES IN SCHEMA aa__testing
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO reporting_user;
GRANT SELECT ON ALL TABLES IN SCHEMA aa__testing TO reporting_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA aa__testing TO reporting_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA aa__testing TO reporting_user;
    --https://dba.stackexchange.com/questions/173635/postgres-permission-denied-for-schema
    --https://github.com/metabase/metabase/issues/7214

--*
GRANT SELECT ON TABLE usa_mortgage_market.hmda_lar__2017 TO reporting_user;
--*

GRANT USAGE ON SCHEMA v__macro_economic_indicators TO reporting_user;
GRANT SELECT ON TABLE v__macro_economic_indicators.populationestimates__usda_ers_2010_to_2018 TO reporting_user;
GRANT SELECT ON TABLE v__macro_economic_indicators.education__acs_1970_to_2017_5yravgs        TO reporting_user;


-- DOCUMENTATION https://aws.amazon.com/blogs/database/managing-postgresql-users-and-roles

/*------------------------------------------------------ End ---------------------------------------------------------*/