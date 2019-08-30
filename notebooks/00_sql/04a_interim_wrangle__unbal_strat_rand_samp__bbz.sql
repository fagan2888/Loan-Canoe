/*****************************************************************************************************************/
/* Purpose:  (1) Typecast transform & Generate Stratified, Unbalanced Random Samples for each year for ingestion */
/*               + (2) Union all the samples for the interim dataset for additional wrangling with pandas        */                                                                                           */
/*                                                                                                               */
/* Author: Blake Zenuni, Summer 2019                                                                             */
/* Date Created:  Aug 1, 2019                                                                                    */
/* Last Modified: Aub 29, 2019                                                                                   */
/*                                                                                                               */
/*****************************************************************************************************************/


/*---------------------------------------------------------------------------------------------------------*/
      --> NB: Latin abbreviation for NOTA BENE, meaning "note well" <--
/*---------------------------------------------------------------------------------------------------------*/


 -- NB1: Stratified random samples are bucketed by household income (strata) + balanced two ways (see below).
 -- NB2: This script is for unbalanced outcomes; script 04b applies this same logic but for balanced outcome
 -- NB3: Bal




/*================================ 04a. Stratified Random samples for HMDA 2009-2010 =================================*/
/*============================================= END 03a - SQL Script  ================================================*/
