/******************************************************************************************************************/
/* Purpose:  (1) Typecast transform & Generate Stratified, Balanced, Random Samples for each year for ingestion   */
/*               + (2) Union all the samples for the interim dataset for additional wrangling with pandas         */                                                                                           */
/*                                                                                                                */
/* Author: Blake Zenuni, Summer 2019                                                                              */
/* Date Created:  Aug 1, 2019                                                                                     */
/* Last Modified: Aub 30, 2019                                                                                    */
/*                                                                                                                */
/******************************************************************************************************************/


/*---------------------------------------------------------------------------------------------------------*/
      --> NB: Latin abbreviation for NOTA BENE, meaning "note well" <--
/*---------------------------------------------------------------------------------------------------------*/


 -- NB1: In this SQL script, stratified random samples are Balanced for outcomes (see CTEs below).

 -- NB2: This random sample subset of the data is stratified by median household income:
       --> i.   We divide the population into different household median income subgroups, or strata;
       --> ii.  We apply statistical measurements of perentiles validate our strata (see below in CTEs);
       --> iii. Random samples are taken from each percentile in proportion to the population, from each of the strata.

    --> Documentation:
             --http://www.wagonhq.com/sql-tutorial/calculating-percentiles-sql
             --https://blog.usejournal.com/creating-an-unbiased-test-set-for-your
--                     -model-using-stratified-sampling-technique-672b778022d5


/*========================== 04b. Stratified Random Samples, Balanced for HMDA 2009-2010 =============================*/
/*============================================= END 04b - SQL Script  ================================================*/

*note to self: add logic flow with partition over() here
