SELECT
  *, pg_size_pretty(total_bytes) AS total
  , pg_size_pretty(index_bytes) AS INDEX
  , pg_size_pretty(toast_bytes) AS toast
  , pg_size_pretty(table_bytes) AS TABLE
FROM (
  SELECT
    *, total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
  FROM (
    SELECT
      c.oid, nspname AS table_schema, relname AS TABLE_NAME
      , c.reltuples AS row_estimate
      , pg_total_relation_size(c.oid) AS total_bytes
      , pg_indexes_size(c.oid) AS index_bytes
      , pg_total_relation_size(reltoastrelid) AS toast_bytes
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE relkind = 'r'
  ) a
) a
ORDER BY table_bytes desc
;




--- for new file to push in github: raw datasets so look at ordering in sql folder, then export 100random raw file in data_structure
CREATE SCHEMA raw_datasets ;
CREATE ROLE reporting_user WITH LOGIN PASSWORD 'team_loan_canoe2019' ;
GRANT USAGE ON SCHEMA raw_datasets TO reporting_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_datasets TO reporting_user ;


/*--- HMDA 2009 ---*/
SELECT *
  INTO raw_datasets.hmda_lar_2009_raw_rand_lm100
  FROM hmda_lar_2009_allrecords
  ORDER BY random()
LIMIT 100
;
/*---*/


/*--- HMDA 2010 ---*/
SELECT *
  INTO raw_datasets.hmda_lar_2010_raw_rand_lm100
  FROM hmda_lar_2010_allrecords
  ORDER BY random()
LIMIT 100
;
/*---*/

/*--- HMDA 2011 ---*/
SELECT *
  INTO raw_datasets.hmda_lar_2011_raw_rand_lm100
  FROM hmda_lar_2011_allrecords
  ORDER BY random()
LIMIT 100
;
/*---*/




/*---------------------------------------------- bz_loancanoe console ------------------------------------------------*/

--
SELECT temp_files AS "Temporary files"
     , temp_bytes AS "Size of temporary files"
FROM   pg_stat_database db;

CREATE TABLE article(article_code bigint, created_at timestamp with time zone, summary text, content text) ;
SELECT pg_sleep(2);
SELECT generate_series(1,10000000) as test;

select table_schema, round(sum(data_length+index_length)/1024/1024/1024,2) "size in GB"
  from information_schema.tables group by 1 order by 2 desc;

  --> all databases
SELECT
    pg_database.datname,
    pg_size_pretty(pg_database_size(pg_database.datname)) AS size
    FROM pg_database;
;
  --

  --> single database
SELECT
    pg_size_pretty (
        pg_database_size ('paddle_loan_canoe')
    );
  --


 --> five largest tables
SELECT
    relname AS "relation",
    pg_size_pretty (
        pg_total_relation_size (C .oid)
    ) AS "total_size"
FROM
    pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C .relnamespace)
WHERE
    nspname NOT IN (
        'pg_catalog',
        'information_schema'
    )
AND C .relkind <> 'i'
AND nspname !~ '^pg_toast'
ORDER BY
    pg_total_relation_size (C .oid) DESC
LIMIT 5;
  --
select * from usa_mortgage_market.hmda_lar_2015_allrecords limit 1;

-----SHOW BINARY LOGS------
CREATE USER blake_zen;
SELECT usename FROM pg_user;

ALTER USER blake_zen WITH SUPERUSER;

select *
  from pg_ls_dir('paddle_loan_canoe')
;
-----

SELECT     pg_size_pretty(pg_total_relation_size(C.oid)) AS "total_size"
  FROM pg_class C
  LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
  WHERE nspname NOT IN ('pg_catalog', 'information_schema')
    AND C.relkind <> 'i'
    AND nspname !~ '^pg_toast'
  ORDER BY pg_total_relation_size(C.oid) DESC
  LIMIT 20
;
/*----*/


-- Autovacuum and transactions
show autovacuum ;

  -- backend activities, i.e. long running transaction that has not been closed
SELECT pid, datname, usename, state, backend_xmin
  FROM pg_stat_activity
  WHERE backend_xmin IS NOT NULL
  ORDER BY age(backend_xmin) DESC
;
SELECT * FROM pg_stat_activity WHERE state = 'active';
SELECT pg_cancel_backend(15906); --pid of the process
SELECT pg_terminate_backend(15906); --if process cannot be killed
  --
  --prepared transactions which have not been committed
SELECT gid, prepared, owner, database, transaction
  FROM pg_prepared_xacts
  ORDER BY age(transaction) DESC
    /*User COMMIT PREPARED or ROLLBACK PREPARED to close them.*/
;
  --

  --replication slots which are not used; find them:
SELECT slot_name, slot_type, database, xmin
  FROM pg_replication_slots
  ORDER BY age(xmin) DESC
  /*Use pg_drop_replication_slot() to delete an unused replication slot.*/
;


/*----*/




/*--------------------------------------------------------------------------------------------------------------------*/



/*----------------------------------------------------- HMDA 2012 ----------------------------------------------------*/
IF OBJECT_ID('interim_datasets.hmda_lar_2012_simplerand25k') EXISTS
  DROP TABLE interim_datasets.hmda_lar_2012_simplerand25k
;
SELECT
  CAST( CASE WHEN hm12.action_taken_name In ( 'Application approved but not accepted', 'Loan originated',
                                              'Loan purchased by the institution' ) THEN 1
             WHEN hm12.action_taken_name = 'Application denied by financial institution' THEN 0
        END As INT) As action_taken, hm12.as_of_year As action_year,
  CAST( CAST( CASE WHEN hm12.tract_to_msamd_income IS NULL THEN NULL ELSE hm12.tract_to_msamd_income END
                As Varchar(5)) As NUMERIC ) As tract_to_masamd_income, hm12.population,
  ROUND(hm12.minority_population, 2) As min_pop_perc, hm12.number_of_owner_occupied_units As num_owoc_units,
  hm12.number_of_1_to_4_family_units As num_1to4_fam_units, hm12.loan_amount_000s As ln_amt_000s,
  hm12.hud_median_family_income As hud_med_fm_inc,
  CAST( CAST( CASE WHEN hm12.applicant_income_000s IS NULL THEN NULL ELSE hm12.applicant_income_000s END
                As Varchar(5) ) As INT) As applic_inc_000s,
  CAST(hm12.owner_occupancy_name As VARCHAR(128)) As own_occ_nm,
  CAST(hm12.loan_type_name As VARCHAR(56)) As ln_type_nm, CAST(hm12.lien_status_name As VARCHAR(56)) As lien_status_nm,
  CAST(hm12.hoepa_status_name As VARCHAR(56)) As hoep_status_nm,
  CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As co_appl_sex,
  CAST(hm12.co_applicant_race_name_1 As VARCHAR(28)) As co_appl_race,
  CAST(hm12.co_applicant_ethnicity_name As VARCHAR(28)) As co_appl_ethn,
  CAST(hm12.co_applicant_sex_name As VARCHAR(28)) As applic_sex,
  CAST(hm12.applicant_race_name_1 As VARCHAR(28)) As applic_race,
  CAST(hm12.applicant_ethnicity_name As VARCHAR(28)) As applic_ethn,
  CAST(hm12.agency_abbr As VARCHAR(28)) As agency_abbr
  INTO interim_datasets.hmda_lar_2012_simplerand25k
  FROM hmda_lar_2012_allrecords hm12
  WHERE hm12.action_taken_name
          In ( 'Application approved but not accepted', 'Application denied by financial institution',
               'Loan originated', 'Loan purchased by the institution')
  ORDER BY random() LIMIT 25000
;
/*--------------------------- end HMDA 2012 ---------------------------*/








--- for new file to push in github: raw datasets so look at ordering in sql folder, then export 100random raw file in data_structure
CREATE SCHEMA raw_datasets ;
CREATE ROLE reporting_user WITH LOGIN PASSWORD 'team_loan_canoe2019' ;
GRANT USAGE ON SCHEMA raw_datasets TO reporting_user ;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_datasets TO reporting_user ;


/*--- HMDA 2009 ---*/
SELECT *
  INTO raw_datasets.hmda_lar_2009_raw_rand_lm100
  FROM hmda_lar_2009_allrecords
  ORDER BY random()
LIMIT 100
;
/*---*/


/*--- HMDA 2010 ---*/
SELECT *
  INTO raw_datasets.hmda_lar_2010_raw_rand_lm100
  FROM hmda_lar_2010_allrecords
  ORDER BY random()
LIMIT 100
;
/*---*/

/*--- HMDA 2012 ---*/
SELECT *
  INTO raw_datasets.hmda_lar_2012_raw_rand_lm100
  FROM  hmda_lar_2012_allrecords
  ORDER BY random()
LIMIT 100
;
/*---*/