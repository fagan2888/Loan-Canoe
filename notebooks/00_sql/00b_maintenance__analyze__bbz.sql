
/*------------------------------------------------ DBMS Maintenance --------------------------------------------------*/

/******** This script presents a wide variety of scripts I use for database analyze, maintenance, and  clean-up *******/
   -- important not only for storage monitoring, but also performance optimization and systems analytics.

    /* Author: Blake Zenuni, Aug. 2019*/


--quick spot checks on one table
select loan_amount_000s
  from paddle_loan_canoe.usa_mortgage_market.hmda_lar_2015_allrecords
  where loan_amount_000s <100000
;
select * from paddle_loan_canoe.usa_mortgage_market.hmda_la_2008_allrecordsr order by random() limit 50000 ;
--

--sophisticated queries for temporary files maintenance
SELECT temp_files AS "Temporary files"
     , temp_bytes AS "Size of temporary files"
FROM   pg_stat_database db;

CREATE TABLE article(article_code bigint, created_at timestamp with time zone, summary text, content text) ;
SELECT pg_sleep(2);
SELECT generate_series(1,10000000) as test;

select table_schema, round(sum(data_length+index_length)/1024/1024/1024,2) "size in GB"
  from information_schema.tables group by 1 order by 2 desc;
--

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


-----SHOW BINARY LOGS------
CREATE USER blake_zen;
SELECT usename FROM pg_user;

ALTER USER blake_zen WITH SUPERUSER;

select *
  from pg_ls_dir('paddle_loan_canoe')
;
--
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


--> Autovacuum and transactions
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

--> replication slots which are not used; find them:
SELECT slot_name, slot_type, database, xmin
  FROM pg_replication_slots
  ORDER BY age(xmin) DESC
  /*Use pg_drop_replication_slot() to delete an unused replication slot.*/
;
--


-->additional query to determine size of db
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
) a;
--
/*----*/
/*--------------------------------------------------------------------------------------------------------------------*/
