--DROP TABLE    mumh_tdim_202307  PURGE;
CREATE TABLE  mumh_tdim_202307  COMPRESS FOR QUERY HIGH AS

WITH  tdim  as  (

select
  year_month
  ,financial_year
  ,calendar_year
  ,financial_quarter_ext
  ,financial_year||' Q'||financial_quarter				      as	financial_quarter
  ,count(year_month)	over(partition by	financial_year)	as	ym_count
  ,count(year_month)  over(partition by financial_quarter_ext) as  qtr_count
from
  dim.year_month_dim
where 1  =  1
  and year_month  between 201504  and mgmt.pkg_public_dwh_functions.f_get_latest_period('EPACT2')
) 

select * from tdim where qtr_count = 3
;

--DROP TABLE    mumh_drug_dim_202307  PURGE;
CREATE TABLE  mumh_drug_dim_202307  COMPRESS FOR QUERY HIGH AS

SELECT
  cdr.year_month
  ,record_id
  ,bnf_chapter
  ,chapter_descr
  ,bnf_section
  ,section_descr
  ,bnf_paragraph
  ,paragraph_descr
  ,bnf_sub_paragraph
  ,sub_paragraph_descr
  ,bnf_chemical_substance
  ,chemical_substance_bnf_descr
  ,bnf_product_level
  ,bnf_product_level_descr
  ,presentation_bnf
  ,presentation_bnf_descr
FROM
  dim.cdr_ep_drug_bnf_dim cdr
INNER JOIN
  mawil.mumh_tdim_202307  tdim
  ON  cdr.year_month  = tdim.year_month
WHERE 1 = 1
  AND bnf_section in  ('0401', '0402', '0403', '0404', '0411')
;

DROP TABLE    mumh_org_dim_202307 PURGE;
CREATE TABLE  mumh_org_dim_202307 COMPRESS FOR QUERY HIGH AS

WITH

max_org AS  (
  SELECT
    MAX(year_month) AS  m_year_month
    ,lvl_5_oupdt
    ,lvl_5_ou
  FROM
    dim.hs_dy_level_5_flat_dim
  WHERE 1 = 1
    AND hs_ctry_ou  = 1
    AND data_added_by_dental  = 'N'
    AND lvl_5_oupdt > 0
  GROUP BY
    lvl_5_oupdt
    ,lvl_5_ou
)

,pcd  AS  (
  SELECT
    year_month
    ,lvl_5_oupdt
    ,lvl_5_ou
    ,CASE WHEN  lvl_5_ou  < 0 THEN  NULL  
          ELSE  LAST_VALUE(UPPER(REPLACE(lvl_5_hist_postcode, ' ', '')))  IGNORE NULLS  OVER(PARTITION BY lvl_5_oupdt, lvl_5_ou, lvl_5_ltst_alt_cde ORDER BY  year_month)
          END AS  lvl_5_cur_postcode
  FROM
    dim.hs_dy_level_5_flat_dim
  WHERE 1 = 1
    AND hs_ctry_ou  = 1
    AND data_added_by_dental  = 'N'
    AND lvl_5_oupdt > 0
)

,cur_pcd  AS  (
  SELECT
    pcd.lvl_5_oupdt
    ,pcd.lvl_5_ou
    ,pcd.lvl_5_cur_postcode
  FROM
    pcd
  INNER JOIN
    max_org
    ON  pcd.lvl_5_oupdt = max_org.lvl_5_oupdt
    AND pcd.lvl_5_ou    = max_org.lvl_5_ou
    AND pcd.year_month  = max_org.m_year_month
)

,org  AS  (
  SELECT
    l5.lvl_5_oupdt
    ,l5.lvl_5_ou
    ,l5.frmttd_lvl_5_ltst_nm
    ,l5.lvl_5_ltst_alt_cde
    ,cur_pcd.lvl_5_cur_postcode
    ,UPPER(REPLACE(ons.pcd, ' ', ''))         AS  pcd
    ,ons.lsoa11 AS  practice_lsoa_code
--    ,NVL(TO_CHAR(TO_NUMBER(ons.imd_decile)), 'Unknown')	AS	imd_decile
--		,NVL(TO_CHAR(TO_NUMBER(ons.imd_rank)), 'Unknown')	  AS	imd_rank
    ,TO_NUMBER(ons.imd_decile)	AS	imd_decile_prac
    ,TO_NUMBER(ons.imd_rank)	  AS	imd_rank_prac
    ,case 	when	l5.cur_area_ltst_clsd = 'Y' 	or 	l5.cur_area_team_ltst_nm	in	('ENGLISH/WELSH DUMMY DENTAL','UNIDENTIFIED DEPUTISING SERVICES','UNIDENTIFIED DOCTORS')	then	'UNKNOWN ICB'
				else 	l5.cur_frmttd_area_team_ltst_nm	end		as	icb_name
		,case 	when	l5.cur_area_ltst_clsd = 'Y' 	or 	l5.cur_area_team_ltst_nm	in	('ENGLISH/WELSH DUMMY DENTAL','UNIDENTIFIED DEPUTISING SERVICES','UNIDENTIFIED DOCTORS')	then	'-'
				else 	l5.cur_area_team_ltst_alt_cde	end		as	icb_code
		,case 	when	l5.cur_region_ltst_clsd = 'Y' 	or 	l5.cur_region_ltst_nm		in	('ENGLISH/WELSH DUMMY DENTAL','UNIDENTIFIED DEPUTISING SERVICES','UNIDENTIFIED DOCTORS')	then	'UNKNOWN REGION'
				else 	l5.cur_frmttd_region_ltst_nm	end		as	region_name
		,case 	when	l5.cur_region_ltst_clsd = 'Y' 	or 	l5.cur_region_ltst_nm		in	('ENGLISH/WELSH DUMMY DENTAL','UNIDENTIFIED DEPUTISING SERVICES','UNIDENTIFIED DOCTORS')	then	'-'
				else 	l5.cur_region_ltst_alt_cde		end		as	region_code
  FROM
    dim.cur_ep_level_5_flat_dim l5
  INNER JOIN
    cur_pcd
    ON  l5.lvl_5_oupdt  = cur_pcd.lvl_5_oupdt
    AND l5.lvl_5_ou     = cur_pcd.lvl_5_ou
  LEFT OUTER JOIN
    grali.ons_nspl_may_23 ons
    ON  cur_pcd.lvl_5_cur_postcode  = UPPER(REPLACE(ons.pcd, ' ', ''))
)

SELECT  * FROM  org
;

--DROP TABLE    mumh_fact_202307  PURGE;
CREATE TABLE  mumh_fact_202307  COMPRESS FOR  QUERY HIGH  AS

WITH  fact  AS  (
  
  SELECT
    fact.identified_patient_id
    ,tdim.financial_year
  --  ,tdim.calendar_year
  --  ,tdim.financial_quarter
    ,fact.year_month
    ,org.region_name
    ,org.region_code
    ,org.icb_name
    ,org.icb_code
    ,org.frmttd_lvl_5_ltst_nm
    ,org.lvl_5_ltst_alt_cde
    ,org.lvl_5_cur_postcode
    ,org.imd_decile_prac
    ,org.imd_rank_prac
    ,org.practice_lsoa_code
    ,drug.bnf_section
    ,drug.section_descr
    ,drug.bnf_paragraph
    ,drug.paragraph_descr
    ,drug.bnf_chemical_substance
    ,drug.chemical_substance_bnf_descr
    ,drug.presentation_bnf
    ,drug.presentation_bnf_descr
    --  <methodology change 2023> no longer impute patient flag from PDS data
    ,fact.patient_identified
    ,fact.eps_part_date
    --	calculate age of patient using PDS_DOB at 30th Sept of given year
    ,nvl(trunc((to_number(substr(tdim.financial_year,1,4)||'0930') - to_number(to_char(fact.pds_dob,'YYYYMMDD')))/10000),-1)	AS	calc_age
    ,fact.pds_gender
    ,CASE WHEN  fact.pds_gender = 1 THEN  'Male'
          WHEN  fact.pds_gender = 2 THEN  'Female'
          ELSE  'Unknown' END AS  gender_descr
    ,fact.patient_lsoa_code
    ,sum(item_count)        AS  item_count
    ,sum(item_pay_dr_nic)   AS  item_pay_dr_nic
    ,sum(item_calc_pay_qty) AS  item_calc_pay_qty
  FROM
    aml.px_form_item_elem_comb_fact_av  fact
  INNER JOIN
    mumh_tdim_202307  tdim
    ON  fact.year_month = tdim.year_month
  INNER JOIN
    mumh_org_dim_202307 org
    ON  fact.presc_type_prnt =  org.lvl_5_oupdt
    AND fact.presc_id_prnt   =  org.lvl_5_ou
  INNER JOIN
    mumh_drug_dim_202307  drug
    ON  fact.calc_prec_drug_record_id = drug.record_id
    AND fact.year_month               = drug.year_month
  WHERE 1 = 1
    --  regular exlusions
    AND fact.PAY_DA_END			=	'N' -- excludes disallowed items
    AND fact.PAY_ND_END			=	'N' -- excludes not dispensed items
    AND fact.PAY_RB_END			=	'N' -- excludes referred back items
    AND fact.CD_REQ				=	'N' -- excludes controlled drug requisitions
    AND fact.OOHC_IND			=	0   -- excludes out of hours dispensing
    AND fact.PRIVATE_IND		=	0   -- excludes private dispensers
    AND fact.IGNORE_FLAG		=	'N' -- excludes LDP dummy forms	
    AND	fact.PRESC_TYPE_PRNT	not in	(8,54)	-- excludes private and pharmacy prescribers
    AND fact.mod_flag = 'N'
  GROUP BY
   fact.identified_patient_id
    ,tdim.financial_year
  --  ,tdim.calendar_year
  --  ,tdim.financial_quarter
    ,fact.year_month
    ,org.region_name
    ,org.region_code
    ,org.icb_name
    ,org.icb_code
    ,org.frmttd_lvl_5_ltst_nm
    ,org.lvl_5_ltst_alt_cde
    ,org.lvl_5_cur_postcode
    ,org.imd_decile_prac
    ,org.imd_rank_prac
    ,org.practice_lsoa_code
    ,drug.bnf_section
    ,drug.section_descr
    ,drug.bnf_paragraph
    ,drug.paragraph_descr
    ,drug.bnf_chemical_substance
    ,drug.chemical_substance_bnf_descr
    ,drug.presentation_bnf
    ,drug.presentation_bnf_descr
    ,fact.patient_identified
    ,fact.eps_part_date
    ,nvl(trunc((to_number(substr(tdim.financial_year,1,4)||'0930') - to_number(to_char(fact.pds_dob,'YYYYMMDD')))/10000),-1)
    ,fact.pds_gender
    ,fact.patient_lsoa_code
)

,lsoa AS  (
  SELECT
    fact.*
    ,NVL(LAST_VALUE(patient_lsoa_code IGNORE NULLS) OVER(PARTITION BY identified_patient_id  ORDER BY  year_month, eps_part_date, patient_lsoa_code  NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), practice_lsoa_code) AS lsoa_code
  FROM
    fact
)

,imd AS  (
  SELECT
    lsoa.*
    ,imd.imd_decile
  FROM
    lsoa
  LEFT OUTER JOIN
    mawil.imd_2019  imd
    ON  lsoa.lsoa_code  = imd.lsoa11
)

SELECT
    identified_patient_id
    ,financial_year
  --  ,calendar_year
  --  ,financial_quarter
    ,year_month
    ,region_name
    ,region_code
    ,icb_name
    ,icb_code
    ,frmttd_lvl_5_ltst_nm
    ,lvl_5_ltst_alt_cde
    ,lvl_5_cur_postcode
    ,bnf_section
    ,section_descr
    ,bnf_paragraph
    ,paragraph_descr
    ,bnf_chemical_substance
    ,chemical_substance_bnf_descr
    ,presentation_bnf
    ,presentation_bnf_descr
    ,patient_identified
    ,calc_age
    ,pds_gender
    ,gender_descr
    ,practice_lsoa_code
    ,patient_lsoa_code
    ,lsoa_code
    ,imd_decile_prac
    ,imd_rank_prac
    ,imd_decile
    ,sum(item_count)        AS  item_count
    ,sum(item_pay_dr_nic)   AS  item_pay_dr_nic
    ,sum(item_calc_pay_qty) AS  item_calc_pay_qty
FROM
  imd
WHERE 1 = 1
GROUP BY
    identified_patient_id
    ,financial_year
  --  ,calendar_year
  --  ,financial_quarter
    ,year_month
    ,region_name
    ,region_code
    ,icb_name
    ,icb_code
    ,frmttd_lvl_5_ltst_nm
    ,lvl_5_ltst_alt_cde
    ,lvl_5_cur_postcode
    ,bnf_section
    ,section_descr
    ,bnf_paragraph
    ,paragraph_descr
    ,bnf_chemical_substance
    ,chemical_substance_bnf_descr
    ,presentation_bnf
    ,presentation_bnf_descr
    ,patient_identified
    ,calc_age
    ,pds_gender
    ,gender_descr
    ,practice_lsoa_code
    ,patient_lsoa_code
    ,lsoa_code
    ,imd_decile_prac
    ,imd_rank_prac
    ,imd_decile
;

  