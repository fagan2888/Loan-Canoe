
/*========================Feature Engineering using SQL CTEs, JOINs and Conditional Clauses===========================*/

/*-----Author: Blake Zenuni, Sept. 2019-----*/

--2016 to 2017:
WITH
    engineer_denial_binaries AS --getting pass fail 1/0 of credist scores from denial data entries
    ( SELECT hm.act_outc, hm.action_year,hm.rate_spread,
        --if any denial reason was for credit history we assume failing credit score, else assume passing credit score
        CASE WHEN lower(hm.denials) LIKE '%credit history%' THEN 0 ELSE 1 END cred_scr_pass_fail_flg,
        --if any denial reason was for mortg insur denied we assume failing loan-to-value score, else assume passing
        CASE WHEN lower(hm.denials) LIKE '%mortgage insurance denied%' THEN 0 ELSE 1 END ltv_pass_fail_flg,
        hm.tract_to_msamd_inc, hm.pop, hm.minority_pop_perc, hm.num_owoc_units, hm.num_1to4_fam_units,
        hm.ln_amt_000s, hm.hud_med_fm_inc, hm.applic_inc_000s, hm.state_abbr, hm.property_type_nm,
        hm.own_occ_nm, hm.msamd_nm, hm.lien_status_nm, hm.hoep_status_nm, hm.co_appl_sex, hm.co_appl_race,
        hm.co_appl_ethn, hm.applic_sex, hm.applic_race, hm.applic_ethn, hm.agency_abbr
      FROM interim_datasets_v2.hmda_2016_2017_union_srandom_bal_50k hm
      WHERE msamd_nm != ''--note to self hidden character here, doesn't affect things but better to clean it if possible
    ) ,
    engineer_msa_entity_emb AS --embedding msa as a continuous numeric value based on frequency an msa appears in data
    ( SELECT msamd_nm,
             Count(msamd_nm)*100.0/(sum(count(*)) over()) as  msa_ent_emb_score --most efficient
      FROM interim_datasets_v2.hmda_2016_2017_union_srandom_bal_50k hm
      WHERE msamd_nm != ''
      GROUP BY msamd_nm
    )
SELECT hm.act_outc, hm.action_year,hm.rate_spread, hm.cred_scr_pass_fail_flg, hm.ltv_pass_fail_flg,
       ROUND(eng_m.msa_ent_emb_score, 2) as msa_ent_emb_score, hm.tract_to_msamd_inc, hm.pop, hm.minority_pop_perc,
       hm.num_owoc_units, hm.num_1to4_fam_units,hm.ln_amt_000s, hm.hud_med_fm_inc, hm.applic_inc_000s, hm.state_abbr,
       hm.property_type_nm, hm.own_occ_nm, hm.msamd_nm, hm.lien_status_nm, hm.hoep_status_nm, hm.co_appl_sex,
       hm.co_appl_race, hm.co_appl_ethn, hm.applic_sex, hm.applic_race, hm.applic_ethn, hm.agency_abbr
INTO interim_datasets_v2.hmda_2016_2017_union_srandom_bal_50k_eng
FROM engineer_denial_binaries hm
LEFT OUTER JOIN engineer_msa_entity_emb eng_m ON hm.msamd_nm = eng_m.msamd_nm
;
--




--All Sample Years 2010 - 2017:
WITH
    engineer_denial_binaries AS --getting pass fail 1/0 of credist scores from denial data entries
    ( SELECT hm.act_outc, hm.action_year,hm.rate_spread,
        --if any denial reason was for credit history we assume failing credit score, else assume passing credit score
        CASE WHEN lower(hm.denials) LIKE '%credit history%' THEN 0 ELSE 1 END cred_scr_pass_fail_flg,
        --if any denial reason was for mortg insur denied we assume failing loan-to-value score, else assume passing
        CASE WHEN lower(hm.denials) LIKE '%mortgage insurance denied%' THEN 0 ELSE 1 END ltv_pass_fail_flg,
        hm.tract_to_msamd_inc, hm.pop, hm.minority_pop_perc, hm.num_owoc_units, hm.num_1to4_fam_units,
        hm.ln_amt_000s, hm.hud_med_fm_inc, hm.applic_inc_000s, hm.state_abbr, hm.property_type_nm,
        hm.own_occ_nm, hm.msamd_nm, hm.lien_status_nm, hm.hoep_status_nm, hm.co_appl_sex, hm.co_appl_race,
        hm.co_appl_ethn, hm.applic_sex, hm.applic_race, hm.applic_ethn, hm.agency_abbr
      FROM paddle_loan_canoe.interim_datasets_v2.interim_hmda_2010_2017_simplerand_balanced200k hm
      WHERE msamd_nm != ''--note to self hidden character here, doesn't affect things but better to clean it if possible
    ) ,
    engineer_msa_entity_emb AS --embedding msa as a continuous numeric value based on frequency an msa appears in data
    ( SELECT msamd_nm,
             Count(msamd_nm)*100.0/(sum(count(*)) over()) as  msa_ent_emb_score --most efficient
      FROM paddle_loan_canoe.interim_datasets_v2.interim_hmda_2010_2017_simplerand_balanced200k hm
      WHERE msamd_nm != ''
      GROUP BY msamd_nm
    )
SELECT hm.act_outc, hm.action_year,hm.rate_spread, hm.cred_scr_pass_fail_flg, hm.ltv_pass_fail_flg,
       ROUND(eng_m.msa_ent_emb_score, 2) as msa_ent_emb_score, hm.tract_to_msamd_inc, hm.pop, hm.minority_pop_perc,
       hm.num_owoc_units, hm.num_1to4_fam_units,hm.ln_amt_000s, hm.hud_med_fm_inc, hm.applic_inc_000s, hm.state_abbr,
       hm.property_type_nm, hm.own_occ_nm, hm.msamd_nm, hm.lien_status_nm, hm.hoep_status_nm, hm.co_appl_sex,
       hm.co_appl_race, hm.co_appl_ethn, hm.applic_sex, hm.applic_race, hm.applic_ethn, hm.agency_abbr
INTO interim_datasets_v2.interim_hmda_2010_2017_union_srandom_bal_200k_eng
FROM engineer_denial_binaries hm
LEFT OUTER JOIN engineer_msa_entity_emb eng_m ON hm.msamd_nm = eng_m.msamd_nm
;


/*====================================================================================================================*/





/----------------------------------------------------*Scrap notes*-----------------------------------------------------/

/*--this was the old SQL query, worked but not most efficient--*/
SELECT msamd_nm,
       Count(msamd_nm)*100.0/(sum(count(*)) over()) as  msa_ent_emb_score --most efficient
FROM interim_datasets_v2.hmda_2016_2017_union_srandom_bal_50k hm
WHERE msamd_nm != ''
GROUP BY msamd_nm
/*--------------------------------------------------------------------------------------------------------------------*/