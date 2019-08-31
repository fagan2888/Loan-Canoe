/**********************************************************************************************************************/
/* Purpose:  (1) Generate raw data rand samples for all yrs up to 100 rows + union all years                          */                                                                                           */
/*                                                                                                                    */
/* Author: Blake Zenuni, Summer 2019                                                                                  */
/* Date Created:  Aug 01, 2019                                                                                        */
/* Last Modified: Aub 30, 2019                                                                                        */
/*                                                                                                                    */
/**********************************************************************************************************************/


/*---------------------------------------------------------------------------------------------------------*/
      --> NB: Latin abbreviation for NOTA BENE, meaning "note well" <--
/*---------------------------------------------------------------------------------------------------------*/


 -- NB1: This script generates the first 100 rows from each HMDA year to output to github repo for accessibility.

 /* NB2: To aid in replicability, we present these outputs to enable the readers to recreate or otherwise leverage our
         analysis using the (albeit very small!) output samples, should they otherwise not have the time to perform our
         expansive SQL ingestion and wrangling processes
*/





/*=================================== 02b. Raw random samples for HMDA 2009-2010 =====================================*/


/***** --- First, create PostgreSQL Cross Database Queries using DbLink --- *****/

    -->install DbLink extension.
CREATE EXTENSION dblink SCHEMA raw_datasets ;
;
    -->verify DbLink
SELECT pg_namespace.nspname, pg_proc.proname --verify DbLink
FROM pg_proc, pg_namespace
WHERE pg_proc.pronamespace=pg_namespace.oid AND pg_proc.proname LIKE '%dblink%'
;
    -->test connection of database
SELECT raw_datasets.dblink_connect_u('host=loandata.ckrhcceyaump.us-east-2.rds.amazonaws.com ' ||
                                     'user=reporting_user password=loan_canoe2019 dbname=postgres')
;
    -->read a table from teammate's postgreSQL database within my database
SELECT hm16_simplerand25k.*
FROM raw_datasets.dblink('user=reporting_user dbname=postgres port=5432
             host=loandata.ckrhcceyaump.us-east-2.rds.amazonaws.com password=loan_canoe2019',
            'SELECT hm16.* FROM postgres.interim_datasets.hmda_lar_2016_simplerand25k hm16')
        As hm16_simplerand25k
;
    --> Documentation:
          -- (1). https://stackoverflow.com/questions/50936251/error-function-dblinkunknown-unknown-does-not-exist
          -- (2). http://www.leeladharan.com/postgresql-cross-database-queries-using-dblink

/*-------------*/



/*------Second, Output each SQL data table (from below) to local repo then git commit => git push to remote repo------*/


    ---> z_tn_AWS_paddleloancanoe <---


        /*--- HMDA 2011 ---*/
        SELECT hm11.*
          INTO raw_datasets.hmda_lar_2011_raw_rand_lm100
          FROM public.hmda_lar_2011_allrecords hm11
          ORDER BY random()
        LIMIT 100
        ;
        /*---*/

        /*--- HMDA 2012 ---*/
        SELECT hm12.*
          INTO raw_datasets.hmda_lar_2012_raw_rand_lm100
          FROM public.hmda_lar_2012_allrecords hm12
          ORDER BY random()
        LIMIT 100
        ;
        /*---*/

        /*--- HMDA 2013 ---*/
        SELECT hm13.*
          INTO raw_datasets.hmda_lar_2013_raw_rand_lm100
          FROM public.hmda_lar_2013_allrecords hm13
          ORDER BY random()
        LIMIT 100
        ;
        /*---*/


        /*------------------------------------------ Union 2011-2013 ------------------------------------------*/

            -->generate datatype of fields as this is important otherwise UNION will error out
            Select hm11.column_name, hm11.data_type, hm12.column_name, hm12.data_type,
                   hm13.column_name, hm13.data_type
            From( Select column_name, data_type From information_schema.columns
                  Where table_name = 'hmda_lar_2011_raw_rand_lm100') hm11
            Left Outer Join
                ( Select column_name, data_type From information_schema.columns
                  where table_name = 'hmda_lar_2012_raw_rand_lm100') hm12
                On hm12.column_name = hm11.column_name
            Left Outer Join
                ( Select column_name, data_type From information_schema.columns
                  where table_name = 'hmda_lar_2013_raw_rand_lm100') hm13
                On hm11.column_name = hm13.column_name
            ;
            --*
            ;
            WITH
                union_hmda_2011_to_2013 AS
                ( SELECT hm11.* FROM raw_datasets.hmda_lar_2011_raw_rand_lm100 hm11
                      UNION ALL
                  SELECT hm12.* FROM raw_datasets.hmda_lar_2012_raw_rand_lm100 hm12
                      UNION ALL
                  SELECT hm13.* FROM raw_datasets.hmda_lar_2012_raw_rand_lm100 hm13
                )
          SELECT hm_u.* INTO raw_datasets.hmda_union_2011_to_2013_raw_rand_lm300
          FROM union_hmda_2011_to_2013 hm_u
            ;
        /*----------------------------------------------------------------------------------------------------*/



    ---> z_bz_AWS_paddleloancanoe <---


        /*--- HMDA 2010 ---*/
        SELECT hm10.*
          INTO raw_datasets.hmda_lar_2010_raw_rand_lm100
          FROM ppaddle_loan_canoe.usa_mortgage_market.hmda_lar_2010_allrecords hm10
          ORDER BY random()
        LIMIT 100
        ;
        /*---*/


        /*--- HMDA 2014 ---*/
        SELECT hm14.*
          INTO raw_datasets.hmda_lar_2014_raw_rand_lm100
          FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2014_allrecords hm14
          ORDER BY random()
        LIMIT 100
        ;
        /*---*/

        /*--- HMDA 2015 ---*/
        SELECT hm15.*
          INTO raw_datasets.hmda_lar_2015_raw_rand_lm100
          FROM paddle_loan_canoe.usa_mortgage_market.hmda_lar_2015_allrecords hm15
          ORDER BY random()
        LIMIT 100
        ;
        /*---*/


        /*--------------------------------------- Union 2010 & 2014-2015 ---------------------------------------*/

            -->generate datatype of fields as this is important otherwise UNION will error out
            Select hm10.column_name, hm10.data_type, hm14.column_name, hm14.data_type,
                   hm15.column_name, hm15.data_type
            From( Select column_name, data_type From information_schema.columns
                  Where table_name = 'hmda_lar_2010_raw_rand_lm100') hm10
            Left Outer Join
                ( Select column_name, data_type From information_schema.columns
                  where table_name = 'hmda_lar_2014_raw_rand_lm100') hm14
                On hm10.column_name = hm14.column_name
            Left Outer Join
            ( Select column_name, data_type From information_schema.columns
                  where table_name = 'hmda_lar_2015_raw_rand_lm100') hm15
                On hm10.column_name = hm15.column_name
            ;
            --*
            ;
            WITH
                union_hmda_2010_2014_2015 AS
                ( SELECT hm10.* FROM raw_datasets.hmda_lar_2010_raw_rand_lm100 hm10
                      UNION ALL
                  SELECT hm14.* FROM raw_datasets.hmda_lar_2014_raw_rand_lm100 hm14
                      UNION ALL
                  SELECT hm15.* FROM raw_datasets.hmda_lar_2015_raw_rand_lm100 hm15
                )
            SELECT hm_u.* INTO raw_datasets.hmda_union_2014_2015_raw_rand_lm300
            FROM union_hmda_2010_2014_2015 hm_u
            ;
        /*-----------------------------------------------------------------------------------------------------*/



   ---> z_ak_AWS_paddleloancanoe <---

        /*--- HMDA 2016 ---*/
        SELECT hm16.*
          INTO raw_datasets.hmda_lar_2016_raw_rand_lm100
          FROM public.hmda_lar_2016_allrecords hm16
          ORDER BY random()
        LIMIT 100
        ;
        /*---*/

        /*--- HMDA 2017 ---*/
        SELECT hm17.*
          INTO raw_datasets.hmda_lar_2017_raw_rand_lm100
          FROM public.hmda_lar_2017_allrecords hm17
          ORDER BY random()
        LIMIT 100
        ;
        /*---*/


        /*------------------------------------------ Union 2016-2017 ------------------------------------------*/

            -->generate datatype of fields as this is important otherwise UNION will error out
            Select hm16.column_name, hm16.data_type, hm17.column_name, hm17.data_type
            From( Select column_name, data_type From information_schema.columns
                  Where table_name = 'hmda_lar_2016_raw_rand_lm100') hm16
            Left Outer Join
                ( Select column_name, data_type From information_schema.columns
                  where table_name = 'hmda_lar_2017_raw_rand_lm100') hm17
                On hm16.column_name = hm17.column_name
            ;
            --*
            ;
            WITH
                union_hmda_2016_2017 AS
                ( SELECT hm16.* FROM raw_datasets.hmda_lar_2016_raw_rand_lm100 hm16
                      UNION ALL
                  SELECT hm17.* FROM raw_datasets.hmda_lar_2017_raw_rand_lm100 hm17
                )
            SELECT hm_u.* INTO raw_datasets.hmda_union_2016_2017_raw_rand_lm200
            FROM union_hmda_2016_2017 hm_u
            ;
        /*----------------------------------------------------------------------------------------------------*/




        /*------------------------------------------ UNION ALL Years 2010-2017 ------------------------------------------*/
            ;
            WITH
                union_hmda_2010_to_2017 AS
                ( SELECT hm10.* FROM raw_datasets.hmda_lar_2010_raw_rand_lm100 hm10
                        UNION ALL
                  SELECT hm11.* FROM raw_datasets.hmda_lar_2011_raw_rand_lm100 hm11
                        UNION ALL
                  SELECT hm12.* FROM raw_datasets.hmda_lar_2012_raw_rand_lm100 hm12
                        UNION ALL
                  SELECT hm13.* FROM raw_datasets.hmda_lar_2013_raw_rand_lm100 hm13
                        UNION ALL
                  SELECT hm14.* FROM raw_datasets.hmda_lar_2014_raw_rand_lm100 hm14
                        UNION ALL
                  SELECT hm15.* FROM raw_datasets.hmda_lar_2015_raw_rand_lm100 hm15
                        UNION ALL
                  SELECT hm16.* FROM raw_datasets.hmda_lar_2016_raw_rand_lm100 hm16
                        UNION ALL
                  SELECT hm17.* FROM raw_datasets.hmda_lar_2017_raw_rand_lm100 hm17
                )
            SELECT hm_u.* INTO raw_datasets.hmda_union_2010_to_2017_raw_rand_lm200
            FROM union_hmda_2010_to_2017 hm_u
            ;
        /*----------------------------------------------------------------------------------------------------*/





/*-------------*/

/***============================================ END 02b - SQL Script  =============================================***/


