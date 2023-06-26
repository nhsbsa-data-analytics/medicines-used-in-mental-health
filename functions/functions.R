### data warehouse extracts from fact table

capture_rate_extract_period <- function(con, period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))   
  if (period_type == "year") {
    #filter for year in function call
    fact <- fact_year %>%
      dplyr::group_by(FINANCIAL_YEAR,
                      `BNF section name` = SECTION_NAME,
                      `BNF section code` = SECTION_CODE,
                      PATIENT_IDENTIFIED) %>% 
      dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT, na.rm = T)) %>%
      dplyr::arrange(FINANCIAL_YEAR) %>%
      collect %>%
      tidyr::pivot_wider(names_from = PATIENT_IDENTIFIED,
                         values_from = ITEM_COUNT) %>% 
      mutate(RATE = Y/(Y+N) * 100,
             `BNF section code` = factor(`BNF section code`, 
                                         levels = c("0403","0401","0402","0404","0411"))) %>%
      dplyr::select(-Y, -N) %>% 
      tidyr::pivot_wider(names_from = FINANCIAL_YEAR,
                         values_from = RATE) %>% 
      dplyr:: arrange(`BNF section code`) 
  }
  else  if (period_type == "quarter") {
    
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::group_by(FINANCIAL_QUARTER,
                      `BNF section name` = SECTION_NAME,
                      `BNF section code` = SECTION_CODE,
                      PATIENT_IDENTIFIED) %>% 
      dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT, na.rm = T)) %>%
      dplyr::arrange(FINANCIAL_QUARTER) %>%
      collect %>%
      tidyr::pivot_wider(names_from = PATIENT_IDENTIFIED,
                         values_from = ITEM_COUNT) %>% 
      mutate(RATE = Y/(Y+N) * 100,
             `BNF section code` = factor(`BNF section code`, 
                                         levels = c("0403","0401","0402","0404","0411"))) %>%
      dplyr::select(-Y, -N) %>% 
      tidyr::pivot_wider(names_from = FINANCIAL_QUARTER,
                         values_from = RATE) %>% 
      dplyr:: arrange(`BNF section code`) 
  }
  
  #return data for use in pipeline  
  
  return(fact)
  
}

national_extract_period <- function(con, period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_month <- dplyr::tbl(con,
                           from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  
  if (period_type == "year") {
    
    #filter for year in function call
    
    fact <- fact_year %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_national <- fact %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        SECTION_NAME,
        SECTION_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_YEAR,
        SECTION_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
    
    return(fact_national)
    
  }
  
  else if (period_type == "quarter") {
    
    #filter for quarter in function call
    
    fact <- fact_quarter %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_national <- fact %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        SECTION_NAME,
        SECTION_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_QUARTER,
        SECTION_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
    
  }
  
  else if (period_type == "month") {
    
    #filter for quarter in function call
    
    fact <- fact_month %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        YEAR_MONTH,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_national <- fact %>%
      dplyr::group_by(
        YEAR_MONTH,
        SECTION_NAME,
        SECTION_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        YEAR_MONTH,
        SECTION_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
    
  }  
  
  #return data for use in pipeline
  
  return(fact_national)
  
}

paragraph_extract_period <- function(con, period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_month <- dplyr::tbl(con,
                           from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  
  if (period_type == "year") {
    
    #filter for year in function call
    
    fact <- fact_year %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_paragraph <- fact %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_YEAR,
        SECTION_CODE,
        PARAGRAPH_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
  }
  
  else if (period_type == "quarter") {
    
    #filter for quarter in function call
    
    fact <- fact_quarter %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_paragraph <- fact %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_QUARTER,
        SECTION_CODE,
        PARAGRAPH_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
    
  }
  
  else if (period_type == "month") {
    
    #filter for quarter in function call
    
    fact <- fact_month %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        YEAR_MONTH,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_paragraph <- fact %>%
      dplyr::group_by(
        YEAR_MONTH,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        YEAR_MONTH,
        SECTION_CODE,
        PARAGRAPH_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
    
    
  }  
  
  #return data for use in pipeline
  
  return(fact_paragraph)
  
}

chem_sub_extract_period <- function(con, period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_month <- dplyr::tbl(con,
                           from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  
  if (period_type == "year") {
    
    #filter for year in function call
    
    fact <- fact_year %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_chem_sub <- fact %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_YEAR,
        SECTION_CODE,
        PARAGRAPH_CODE,
        CHEM_SUB_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
    
  }
  
  else if (period_type == "quarter") {
    
    #filter for quarter in function call
    
    fact <- fact_quarter %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_chem_sub <- fact %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_QUARTER,
        SECTION_CODE,
        PARAGRAPH_CODE,
        CHEM_SUB_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
    
  }
  
  else if (period_type == "month") {
    
    #filter for quarter in function call
    
    fact <- fact_month %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        YEAR_MONTH,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_chem_sub <- fact %>%
      dplyr::group_by(
        YEAR_MONTH,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        YEAR_MONTH,
        SECTION_CODE,
        PARAGRAPH_CODE,
        CHEM_SUB_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
    
    
  }  
  
  #return data for use in pipeline
  
  return(fact_chem_sub)
  
}

icb_extract_period <- function(con, period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_month <- dplyr::tbl(con,
                           from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  
  if (period_type == "year") {
    
    #filter for year in function call
    fact <- fact_year %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        REGION_NAME,
        REGION_CODE,
        ICB_NAME,
        ICB_CODE,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_icb <- fact %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        REGION_NAME,
        REGION_CODE,
        ICB_NAME,
        ICB_CODE,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(
        REGION_CODE_ORDER = case_when(
          REGION_CODE == "-" ~ 2,
          TRUE ~ 1
        ),
        ICB_NAME_ORDER = case_when(
          ICB_NAME == "UNKNOWN ICB" ~ 2,
          TRUE ~ 1
        )
      ) %>%
      dplyr::arrange(
        FINANCIAL_YEAR,
        REGION_CODE_ORDER,
        REGION_CODE,
        ICB_NAME_ORDER,
        ICB_NAME,
        SECTION_CODE,
        PARAGRAPH_CODE,
        CHEM_SUB_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      select(
        -REGION_CODE_ORDER,
        -ICB_NAME_ORDER
      )
  }
  else  if (period_type == "quarter") {
    
    #filter for year in function call
    fact <- fact_quarter %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        REGION_NAME,
        REGION_CODE,
        ICB_NAME,
        ICB_CODE,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_icb <- fact %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        REGION_NAME,
        REGION_CODE,
        ICB_NAME,
        ICB_CODE,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(
        REGION_CODE_ORDER = case_when(
          REGION_CODE == "-" ~ 2,
          TRUE ~ 1
        ),
        ICB_NAME_ORDER = case_when(
          ICB_NAME == "UNKNOWN ICB" ~ 2,
          TRUE ~ 1
        )
      ) %>%
      dplyr::arrange(
        FINANCIAL_QUARTER,
        REGION_CODE_ORDER,
        REGION_CODE,
        ICB_NAME_ORDER,
        ICB_NAME,
        SECTION_CODE,
        PARAGRAPH_CODE,
        CHEM_SUB_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      select(
        -REGION_CODE_ORDER,
        -ICB_NAME_ORDER
      )
  }
  else  if (period_type == "month") {
    
    #filter for year in function call
    fact <- fact_month %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        YEAR_MONTH,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        REGION_NAME,
        REGION_CODE,
        ICB_NAME,
        ICB_CODE,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_icb <- fact %>%
      dplyr::group_by(
        YEAR_MONTH,
        REGION_NAME,
        REGION_CODE,
        ICB_NAME,
        ICB_CODE,
        SECTION_NAME,
        SECTION_CODE,
        PARAGRAPH_NAME,
        PARAGRAPH_CODE,
        CHEM_SUB_NAME,
        CHEM_SUB_CODE,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(
        REGION_CODE_ORDER = case_when(
          REGION_CODE == "-" ~ 2,
          TRUE ~ 1
        ),
        ICB_NAME_ORDER = case_when(
          ICB_NAME == "UNKNOWN ICB" ~ 2,
          TRUE ~ 1
        )
      ) %>%
      dplyr::arrange(
        YEAR_MONTH,
        REGION_CODE_ORDER,
        REGION_CODE,
        ICB_NAME_ORDER,
        ICB_NAME,
        SECTION_CODE,
        PARAGRAPH_CODE,
        CHEM_SUB_CODE,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      select(
        -REGION_CODE_ORDER,
        -ICB_NAME_ORDER
      )
  }  
  
  #return data for use in pipeline
  return(fact_icb)
  
}

ageband_extract_period <- function(con, period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))   
  if (period_type == "year") {
    #filter for year in function call
    fact <- fact_year %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        CALC_AGE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_age <- fact %>%
      dplyr::inner_join(
        dplyr::tbl(con,
                   from = dbplyr::in_schema("DIM", "AGE_DIM")),
        by = c(
          "CALC_AGE" = "AGE"
        )
      ) %>%
      dplyr::mutate(
        AGE_BAND = dplyr::case_when(
          is.na(DALL_5YR_BAND) ~ "Unknown",
          TRUE ~ DALL_5YR_BAND
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        SECTION_NAME,
        SECTION_CODE,
        AGE_BAND,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_YEAR,
        SECTION_CODE,
        AGE_BAND,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect}
  else  if (period_type == "quarter") {
    
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        CALC_AGE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_age <- fact %>%
      dplyr::inner_join(
        dplyr::tbl(con,
                   from = dbplyr::in_schema("DIM", "AGE_DIM")),
        by = c(
          "CALC_AGE" = "AGE"
        )
      ) %>%
      dplyr::mutate(
        AGE_BAND = dplyr::case_when(
          is.na(DALL_5YR_BAND) ~ "Unknown",
          TRUE ~ DALL_5YR_BAND
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        SECTION_NAME,
        SECTION_CODE,
        AGE_BAND,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_QUARTER,
        SECTION_CODE,
        AGE_BAND,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect}
  
  #return data for use in pipeline  
  
  return(fact_age)
  
}

gender_extract_period <- function(con, period_type = c("year", "quarter")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))   
  
  if (period_type == "year") {
    #filter for year in function call
    fact <- fact_year %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        GENDER_DESCR,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_gender <- fact %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        SECTION_NAME,
        SECTION_CODE,
        GENDER_DESCR,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_YEAR,
        SECTION_CODE,
        GENDER_DESCR,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect}
  else  if (period_type == "quarter") {
    
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        GENDER_DESCR,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_gender <- fact %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        SECTION_NAME,
        SECTION_CODE,
        GENDER_DESCR,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_QUARTER,
        SECTION_CODE,
        GENDER_DESCR,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      collect
  } 
  
  #return data for use in pipeline   
  
  return(fact_gender)
  
}

age_gender_extract_period <- function(con, period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))   
  if (period_type == "year") {
    #filter for year in function call
    fact <- fact_year %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        CALC_AGE,
        GENDER_DESCR,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) %>%
      ungroup()
    
    fact_age_gender <- fact %>%
      filter(GENDER_DESCR != "Unknown") %>%
      dplyr::inner_join(
        dplyr::tbl(con,
                   from = dbplyr::in_schema("DIM", "AGE_DIM")),
        by = c(
          "CALC_AGE" = "AGE"
        )
      ) %>%
      dplyr::mutate(
        AGE_BAND = dplyr::case_when(
          is.na(DALL_5YR_BAND) ~ "Unknown",
          TRUE ~ DALL_5YR_BAND
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        SECTION_NAME,
        SECTION_CODE,
        AGE_BAND,
        GENDER_DESCR,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_YEAR,
        SECTION_CODE,
        AGE_BAND,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      ungroup() %>%
      collect}
  else  if (period_type == "quarter") {
    
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        CALC_AGE,
        GENDER_DESCR,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) %>%
      ungroup()
    
    fact_age_gender <- fact %>%
      filter(GENDER_DESCR != "Unknown") %>%
      dplyr::inner_join(
        dplyr::tbl(con,
                   from = dbplyr::in_schema("DIM", "AGE_DIM")),
        by = c(
          "CALC_AGE" = "AGE"
        )
      ) %>%
      dplyr::mutate(
        AGE_BAND = dplyr::case_when(
          is.na(DALL_5YR_BAND) ~ "Unknown",
          TRUE ~ DALL_5YR_BAND
        )
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        SECTION_NAME,
        SECTION_CODE,
        AGE_BAND,
        GENDER_DESCR,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_QUARTER,
        SECTION_CODE,
        AGE_BAND,
        desc(PATIENT_IDENTIFIED)
      ) %>%
      ungroup() %>%
      collect}
  
  #return data for use in pipeline  

  return(fact_age_gender)
  
}

imd_extract_period <- function(con, period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "mumh_fact_202307"))   
  if (period_type == "year") {
    #filter for year in function call
    fact <- fact_year %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0),
        IMD_QUINTILE = 
          case_when(
            IMD_DECILE %in% c("1", "2") ~ "1 - Most Deprived",
            IMD_DECILE %in% c("3", "4") ~ "2",
            IMD_DECILE %in% c("5", "6") ~ "3",
            IMD_DECILE %in% c("7", "8") ~ "4",
            IMD_DECILE %in% c("9", "10") ~ "5 - Least Deprived"
          )
        
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        IMD_QUINTILE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_imd <- fact %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
        SECTION_NAME,
        SECTION_CODE,
        IMD_QUINTILE
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_YEAR,
        SECTION_CODE,
        IMD_QUINTILE
      ) %>%
      collect}
  else  if (period_type == "quarter") {
    
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(
          PATIENT_IDENTIFIED == "Y" ~ 1,
          TRUE ~ 0),
        IMD_QUINTILE = 
          case_when(
            IMD_DECILE %in% c("1", "2") ~ "1 - Most Deprived",
            IMD_DECILE %in% c("3", "4") ~ "2",
            IMD_DECILE %in% c("5", "6") ~ "3",
            IMD_DECILE %in% c("7", "8") ~ "4",
            IMD_DECILE %in% c("9", "10") ~ "5 - Least Deprived"
          )
        
      ) %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_NAME,
        SECTION_CODE,
        IMD_QUINTILE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_imd <- fact %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        SECTION_NAME,
        SECTION_CODE,
        IMD_QUINTILE
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        PATIENT_COUNT = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        FINANCIAL_QUARTER,
        SECTION_CODE,
        IMD_QUINTILE
      ) %>%
      collect}
  
  #return data for use in pipeline  
  
  return(fact_imd)
  
}