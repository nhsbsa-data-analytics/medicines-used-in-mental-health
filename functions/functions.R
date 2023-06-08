#function to create time dimension table
create_tdim <- function (
  con,
  start = 201504,
  end = sql("MGMT.PKG_PUBLIC_DWH_FUNCTIONS.f_get_latest_period('EPACT2')")
) {

  # handle start and end arguments to pass as integers
  start <- as.integer(start)

  if(is.numeric(end)) {
    end <- as.integer(end)
  }

  dim <- tbl(con, from = in_schema("DIM", "YEAR_MONTH_DIM")) %>%
    # filter table to time period
    select(YEAR_MONTH, FINANCIAL_YEAR, FINANCIAL_QUARTER) %>%
    group_by(FINANCIAL_YEAR, FINANCIAL_QUARTER) %>%
    # create financial quarter column that sorts correctly
    # calculate month counts to give full quarters only
    mutate(
      FINANCIAL_QUARTER = FINANCIAL_YEAR %||% " Q" %||% FINANCIAL_QUARTER,
      # window functions to perform counts within groups
      MONTH_COUNT = n()
    ) %>%
    # filter to only full financial quarters
    filter(MONTH_COUNT == 3L, YEAR_MONTH >= start, YEAR_MONTH <= end)

}

#function to create org level table with region/ICB information
create_org_dim <- function(con, country = 1) {

  # handle numeric args to pass as integer
  country <- as.integer(country)

  dim <- tbl(con, from = in_schema("DIM", "CUR_EP_LEVEL_5_FLAT_DIM")) %>%
    filter(
      CUR_CTRY_OU %in% country
    ) %>%
    mutate(
      ICB_NAME = case_when(
        CUR_AREA_LTST_CLSD == "Y" ~ "UNKNOWN ICB",
        CUR_AREA_TEAM_LTST_NM %in% c(
          'ENGLISH/WELSH DUMMY DENTAL',
          'UNIDENTIFIED DEPUTISING SERVICES',
          'UNIDENTIFIED DOCTORS'
        ) ~ "UNKNOWN ICB",
        TRUE ~ CUR_FRMTTD_AREA_TEAM_LTST_NM
      ),
      ICB_CODE = case_when(
        CUR_AREA_LTST_CLSD == "Y" ~ "-",
        CUR_AREA_TEAM_LTST_NM %in% c(
          'ENGLISH/WELSH DUMMY DENTAL',
          'UNIDENTIFIED DEPUTISING SERVICES',
          'UNIDENTIFIED DOCTORS'
        ) ~ "-",
        TRUE ~ CUR_AREA_TEAM_LTST_ALT_CDE
      ),
      REGION_NAME = case_when(
        CUR_REGION_LTST_CLSD == "Y" ~ "UNKNOWN REGION",
        CUR_REGION_LTST_NM %in% c(
          'ENGLISH/WELSH DUMMY DENTAL',
          'UNIDENTIFIED DEPUTISING SERVICES',
          'UNIDENTIFIED DOCTORS'
        ) ~ "UNKNOWN REGION",
        TRUE ~ CUR_FRMTTD_REGION_LTST_NM
      ),
      REGION_CODE = case_when(
        CUR_REGION_LTST_CLSD == "Y" ~ "-",
        CUR_REGION_LTST_NM %in% c(
          'ENGLISH/WELSH DUMMY DENTAL',
          'UNIDENTIFIED DEPUTISING SERVICES',
          'UNIDENTIFIED DOCTORS'
        ) ~ "-",
        TRUE ~ CUR_REGION_LTST_ALT_CDE
      ) %>%
    select(
      LVL_5_OUPDT,
      LVL_5_OU,
      ICB_NAME,
      ICB_CODE)
    }

#function to create drug dimension table
create_drug_dim <- function (con, bnf_codes, ...) {

  # handle bnf_codes to allow pattern matching depending on level of BNF
  # searched.
  bnf <- paste0("^(", paste0(bnf_codes, collapse = "|"), ")")

  dim <- tbl(con, from = in_schema("DIM","CDR_EP_DRUG_BNF_DIM")) %>%
    filter(REGEXP_LIKE(PRESENTATION_BNF, bnf)) %>%
    select(
      YEAR_MONTH,
      RECORD_ID,
      PRESENTATION_BNF_DESCR,
      PRESENTATION_BNF,
      CHEMICAL_SUBSTANCE_BNF_DESCR,
      BNF_CHEMICAL_SUBSTANCE,
      PARAGRAPH_DESCR,
      BNF_PARAGRAPH,
      SECTION_DESCR,
      BNF_SECTION,
      CHAPTER_DESCR,
      BNF_CHAPTER,
      ...
    )

}

#function to create fact table in own schema
create_fact <- function (con) {

  fact <- tbl(con, in_schema("AML", "PX_FORM_ITEM_ELEM_COMB_FACT_AV")) %>%
    filter(
      PAY_DA_END == "N", # excludes disallowed items
      PAY_ND_END == "N", # excludes not dispensed items
      PAY_RB_END == "N", # excludes referred back items
      CD_REQ == "N", # excludes controlled drug requisitions
      OOHC_IND == 0L, # excludes out of hours dispensing
      PRIVATE_IND == 0L, # excludes private dispensers
      IGNORE_FLAG == "N", # excludes LDP dummy forms
      PRESC_TYPE_PRNT %NOT IN% c(8L, 54L)
    ) %>%
    select(
      YEAR_MONTH,
      PRESC_TYPE_PRNT,
      PRESC_ID_PRNT,
      CALC_PREC_DRUG_RECORD_ID,
      CALC_AGE,
      IDENTIFIED_PATIENT_ID,
      PDS_DOB,
      PDS_GENDER,
      ITEM_COUNT,
      ITEM_PAY_DR_NIC
    ) %>%
    mutate(
      IDENTIFIED_FLAG = case_when(
        is.null(PDS_DOB) & PDS_GENDER == 0L ~ "N",
        TRUE ~ "Y"
      )
    ) %>%
    group_by(
      YEAR_MONTH,
      PRESC_TYPE_PRNT,
      PRESC_ID_PRNT,
      CALC_PREC_DRUG_RECORD_ID,
      CALC_AGE,
      IDENTIFIED_PATIENT_ID,
      PDS_DOB,
      PDS_GENDER,
      IDENTIFIED_FLAG
    ) %>%
    summarise(
      ITEM_COUNT = sum(ITEM_COUNT),
      ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC),
      .groups = "drop"
    )

}
