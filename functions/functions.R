### data warehouse extracts from fact table

capture_rate_extract_period <- function(con, period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))   
  if (period_type == "year") {
    #filter for year in function call
    fact <- fact_year %>%
      dplyr::group_by(FINANCIAL_YEAR,
                      `BNF Section Name` = SECTION_DESCR,
                      `BNF Section Code` = BNF_SECTION,
                      PATIENT_IDENTIFIED) %>% 
      dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT, na.rm = T)) %>%
      dplyr::arrange(FINANCIAL_YEAR) %>%
      collect %>%
      tidyr::pivot_wider(names_from = PATIENT_IDENTIFIED,
                         values_from = ITEM_COUNT) %>% 
      mutate(RATE = Y/(Y+N) * 100,
             `BNF Section Code` = factor(`BNF Section Code`, 
                                         levels = c("0403","0401","0402","0404","0411"))) %>%
      dplyr::select(-Y, -N) %>% 
      tidyr::pivot_wider(names_from = FINANCIAL_YEAR,
                         values_from = RATE) %>% 
      dplyr:: arrange(`BNF Section Code`) 
  }
  else  if (period_type == "quarter") {
    
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::group_by(FINANCIAL_QUARTER,
                      `BNF Section Name` = SECTION_DESCR,
                      `BNF Section Code` = BNF_SECTION,
                      PATIENT_IDENTIFIED) %>% 
      dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT, na.rm = T)) %>%
      dplyr::arrange(FINANCIAL_QUARTER) %>%
      collect %>%
      tidyr::pivot_wider(names_from = PATIENT_IDENTIFIED,
                         values_from = ITEM_COUNT) %>% 
      mutate(RATE = Y/(Y+N) * 100,
             `BNF Section Code` = factor(`BNF Section Code`, 
                                         levels = c("0403","0401","0402","0404","0411"))) %>%
      dplyr::select(-Y, -N) %>% 
      tidyr::pivot_wider(names_from = FINANCIAL_QUARTER,
                         values_from = RATE) %>% 
      dplyr:: arrange(`BNF Section Code`) 
  }
  
  #return data for use in pipeline  
  
  return(fact)
  
}

national_extract_period <- function(con, period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_month <- dplyr::tbl(con,
                           from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  
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
        SECTION_DESCR,
        BNF_SECTION,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_national <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `BNF Section Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_DESCR,
        BNF_SECTION,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_national <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `BNF Section Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        YEAR_MONTH,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_DESCR,
        BNF_SECTION,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_national <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `Year Month` = YEAR_MONTH,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `Year Month`,
        `BNF Section Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    
  }  
  
  #return data for use in pipeline
  
  return(fact_national)
  
}

population_extract <- function(){
  national_pop <- ons_national_pop(year = c(2015:2021),
                                   area = "ENPOP") %>% 
    mutate(YEAR = as.character(YEAR))
  
  patient_population <- national_extract_period(con, period_type = "year") %>%
    dplyr::select(`Financial Year`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `BNF Section Name`,
                `BNF Section Code`) %>% 
    dplyr::mutate(`Mid-year Population Year` = as.numeric(substr(c(`Financial Year`), 1, 4))) %>%
    dplyr::filter(`Identified Patient Flag` == "Y") %>%
    dplyr::left_join(select(en_ons_national_pop, YEAR, ENPOP), by = c("Mid-year Population Year" = "YEAR")) %>%
    dplyr::mutate(`Patients per 1000 Population` = ((`Total Identified Patients`/ENPOP) * 1000)) %>%
    dplyr::select(`Financial Year`,
                  `Mid-year Population Year`,
                  `BNF Section Name`,
                  `BNF Section Code`,
                  `Total Identified Patients`,
                  `Mid-year Population Estimate` = ENPOP,
                  `Patients per 1000 Population`) %>%
    dplyr::arrange(`Financial Year`,
                   `BNF Section Code`)
  
  return(patient_population)
}

paragraph_extract_period <- function(con, period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_month <- dplyr::tbl(con,
                           from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  
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
        SECTION_DESCR,
        BNF_SECTION,
        PARAGRAPH_DESCR,
        BNF_PARAGRAPH,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_paragraph <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `BNF Paragraph Name` = PARAGRAPH_DESCR,
        `BNF Paragraph Code` = BNF_PARAGRAPH,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_DESCR,
        BNF_SECTION,
        PARAGRAPH_DESCR,
        BNF_PARAGRAPH,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_paragraph <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `BNF Paragraph Name` = PARAGRAPH_DESCR,
        `BNF Paragraph Code` = BNF_PARAGRAPH,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        YEAR_MONTH,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_DESCR,
        BNF_SECTION,
        PARAGRAPH_DESCR,
        BNF_PARAGRAPH,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_paragraph <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `Year Month` = YEAR_MONTH,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `BNF Paragraph Name` = PARAGRAPH_DESCR,
        `BNF Paragraph Code` = BNF_PARAGRAPH,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `Year Month`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    
  }  
  
  #return data for use in pipeline
  
  return(fact_paragraph)
  
}

chem_sub_extract_period <- function(con, period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_month <- dplyr::tbl(con,
                           from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  
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
        SECTION_DESCR,
        BNF_SECTION,
        PARAGRAPH_DESCR,
        BNF_PARAGRAPH,
        CHEMICAL_SUBSTANCE_BNF_DESCR,
        BNF_CHEMICAL_SUBSTANCE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_chem_sub <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `BNF Paragraph Name` = PARAGRAPH_DESCR,
        `BNF Paragraph Code` = BNF_PARAGRAPH,
        `BNF Chemical Substance Name` = CHEMICAL_SUBSTANCE_BNF_DESCR,
        `BNF Chemical Substance Code` = BNF_CHEMICAL_SUBSTANCE,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_DESCR,
        BNF_SECTION,
        PARAGRAPH_DESCR,
        BNF_PARAGRAPH,
        CHEMICAL_SUBSTANCE_BNF_DESCR,
        BNF_CHEMICAL_SUBSTANCE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_chem_sub <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `BNF Paragraph Name` = PARAGRAPH_DESCR,
        `BNF Paragraph Code` = BNF_PARAGRAPH,
        `BNF Chemical Substance Name` = CHEMICAL_SUBSTANCE_BNF_DESCR,
        `BNF Chemical Substance Code` = BNF_CHEMICAL_SUBSTANCE,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        YEAR_MONTH,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_DESCR,
        BNF_SECTION,
        PARAGRAPH_DESCR,
        BNF_PARAGRAPH,
        CHEMICAL_SUBSTANCE_BNF_DESCR,
        BNF_CHEMICAL_SUBSTANCE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_chem_sub <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `Year Month` = YEAR_MONTH,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `BNF Paragraph Name` = PARAGRAPH_DESCR,
        `BNF Paragraph Code` = BNF_PARAGRAPH,
        `BNF Chemical Substance Name` = CHEMICAL_SUBSTANCE_BNF_DESCR,
        `BNF Chemical Substance Code` = BNF_CHEMICAL_SUBSTANCE,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `Year Month`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    
  }  
  
  #return data for use in pipeline
  
  return(fact_chem_sub)
  
}

icb_extract_period <- function(con, period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_month <- dplyr::tbl(con,
                           from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  
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
        SECTION_DESCR,
        BNF_SECTION,
        PARAGRAPH_DESCR,
        BNF_PARAGRAPH,
        CHEMICAL_SUBSTANCE_BNF_DESCR,
        BNF_CHEMICAL_SUBSTANCE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_icb <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Region Name` = REGION_NAME,
        `Region Code` = REGION_CODE,
        `ICB Name` = ICB_NAME,
        `ICB Code` = ICB_CODE,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `BNF Paragraph Name` = PARAGRAPH_DESCR,
        `BNF Paragraph Code` = BNF_PARAGRAPH,
        `BNF Chemical Substance Name` = CHEMICAL_SUBSTANCE_BNF_DESCR,
        `BNF Chemical Substance Code` = BNF_CHEMICAL_SUBSTANCE,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(
        REGION_CODE_ORDER = case_when(
          `Region Code` == "-" ~ 2,
          TRUE ~ 1
        ),
        ICB_NAME_ORDER = case_when(
          `ICB Name` == "UNKNOWN ICB" ~ 2,
          TRUE ~ 1
        )
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        REGION_CODE_ORDER,
        `Region Code`,
        ICB_NAME_ORDER,
        `ICB Name`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Total Identified Patients`)
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        REGION_NAME,
        REGION_CODE,
        ICB_NAME,
        ICB_CODE,
        SECTION_DESCR,
        BNF_SECTION,
        PARAGRAPH_DESCR,
        BNF_PARAGRAPH,
        CHEMICAL_SUBSTANCE_BNF_DESCR,
        BNF_CHEMICAL_SUBSTANCE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_icb <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `Region Name` = REGION_NAME,
        `Region Code` = REGION_CODE,
        `ICB Name` = ICB_NAME,
        `ICB Code` = ICB_CODE,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `BNF Paragraph Name` = PARAGRAPH_DESCR,
        `BNF Paragraph Code` = BNF_PARAGRAPH,
        `BNF Chemical Substance Name` = CHEMICAL_SUBSTANCE_BNF_DESCR,
        `BNF Chemical Substance Code` = BNF_CHEMICAL_SUBSTANCE,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(
        REGION_CODE_ORDER = case_when(
          `Region Code` == "-" ~ 2,
          TRUE ~ 1
        ),
        ICB_NAME_ORDER = case_when(
          `ICB Name` == "UNKNOWN ICB" ~ 2,
          TRUE ~ 1
        )
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        REGION_CODE_ORDER,
        `Region Code`,
        ICB_NAME_ORDER,
        `ICB Name`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Total Identified Patients`)
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        YEAR_MONTH,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        REGION_NAME,
        REGION_CODE,
        ICB_NAME,
        ICB_CODE,
        SECTION_DESCR,
        BNF_SECTION,
        PARAGRAPH_DESCR,
        BNF_PARAGRAPH,
        CHEMICAL_SUBSTANCE_BNF_DESCR,
        BNF_CHEMICAL_SUBSTANCE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_icb <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Finacial Quarter` = FINANCIAL_QUARTER,
        `Year Month` = YEAR_MONTH,
        `Region Name` = REGION_NAME,
        `Region Code` = REGION_CODE,
        `ICB Name` = ICB_NAME,
        `ICB Code` = ICB_CODE,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `BNF Paragraph Name` = PARAGRAPH_DESCR,
        `BNF Paragraph Code` = BNF_PARAGRAPH,
        `BNF Chemical Substance Name` = CHEMICAL_SUBSTANCE_BNF_DESCR,
        `BNF Chemical Substance Code` = BNF_CHEMICAL_SUBSTANCE,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(
        REGION_CODE_ORDER = case_when(
          `Region Code` == "-" ~ 2,
          TRUE ~ 1
        ),
        ICB_NAME_ORDER = case_when(
          `ICB Name` == "UNKNOWN ICB" ~ 2,
          TRUE ~ 1
        )
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Finacial Quarter`,
        `Year Month`,
        REGION_CODE_ORDER,
        `Region Code`,
        ICB_NAME_ORDER,
        `ICB Name`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Identified Patient Flag`)
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
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))   
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
        SECTION_DESCR,
        BNF_SECTION,
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
        `Financial Year` = FINANCIAL_YEAR,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Age Band` = AGE_BAND,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `BNF Section Code`,
        `Age Band`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    }
  
  
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_DESCR,
        BNF_SECTION,
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
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Age Band` = AGE_BAND,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `BNF Section Code`,
        `Age Band`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
    }
  
  #return data for use in pipeline  
  
  return(fact_age)
  
}

gender_extract_period <- function(con, period_type = c("year", "quarter")) {
  
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))   
  
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
        SECTION_DESCR,
        BNF_SECTION,
        GENDER_DESCR,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_gender <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Patient Gender` = GENDER_DESCR,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `BNF Section Code`,
        `Patient Gender`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
  }
  
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_DESCR,
        BNF_SECTION,
        GENDER_DESCR,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_gender <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Patient Gender` = GENDER_DESCR,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `BNF Section Code`,
        `Patient Gender`,
        desc(`Identified Patient Flag`)
      ) %>%
      collect()
  } 
  
  #return data for use in pipeline   
  
  return(fact_gender)
  
}

age_gender_extract_period <- function(con, period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))   
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
        SECTION_DESCR,
        BNF_SECTION,
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
        `Financial Year` = FINANCIAL_YEAR,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Age Band` = AGE_BAND,
        `Patient Gender` = GENDER_DESCR,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `BNF Section Code`,
        `Age Band`,
        desc(`Identified Patient Flag`)
      ) %>%
      ungroup() %>%
      collect()
    }
  
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
        FINANCIAL_YEAR,
        FINANCIAL_QUARTER,
        IDENTIFIED_PATIENT_ID,
        PATIENT_IDENTIFIED,
        SECTION_DESCR,
        BNF_SECTION,
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
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Age Band` = AGE_BAND,
        `Patient Gender` = GENDER_DESCR,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T)
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `BNF Section Code`,
        `Age Band`,
        desc(`Identified Patient Flag`)
      ) %>%
      ungroup() %>%
      collect()
    }
  
  #return data for use in pipeline  

  return(fact_age_gender)
  
}

imd_extract_period <- function(con, period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))   
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
        SECTION_DESCR,
        BNF_SECTION,
        IMD_QUINTILE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T)
      ) 
    
    fact_imd <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `IMD Quintile` = IMD_QUINTILE
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `BNF Section Code`,
        `IMD Quintile`
      ) %>%
      collect()
    }
  
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
        SECTION_DESCR,
        BNF_SECTION,
        IMD_QUINTILE,
        PATIENT_COUNT
      ) %>%
      dplyr::summarise(
        ITEM_COUNT = sum(ITEM_COUNT, na.rm = T),
        ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = T),
        .groups = "drop"
      ) 
    
    fact_imd <- fact %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `IMD Quintile` = IMD_QUINTILE
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Quarter`,
        `BNF Section Code`,
        `IMD Quintile`
      ) %>%
      collect()
    }
  
  #return data for use in pipeline  
  
  return(fact_imd)
  
}

child_adult_extract <- function(con, period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(con,
                          from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))
  fact_quarter <- dplyr::tbl(con,
                             from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307"))   
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
  
  child_adult_split <- fact %>%
    mutate(
      AGE_BAND = case_when(
        CALC_AGE < 0 ~ "Unknown",
        CALC_AGE <= 17 ~ "17 and under",
        TRUE ~ "18 and over"
      )
    ) %>%
    dplyr::group_by(
      `Financial Year` = FINANCIAL_YEAR,
      `BNF Section Name` = SECTION_NAME,
      `BNF Section Code` = SECTION_CODE,
      `Age Band` = AGE_BAND
    ) %>%
    dplyr::summarise(
      `Total Items` = sum(ITEM_COUNT, na.rm = T),
      `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
      `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
      .groups = "drop"
    ) %>%
    dplyr::arrange(
      `Financial Year`,
      `BNF Section Code`,
      `Age Band`
    ) %>%
    collect()
  }
  
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
        FINANCIAL_YEAR,
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
    
    child_adult_split <- fact %>%
      mutate(
        AGE_BAND = case_when(
          CALC_AGE < 0 ~ "Unknown",
          CALC_AGE <= 17 ~ "17 and under",
          TRUE ~ "18 and over"
        )
      ) %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `Financial Quarter` = FINANCIAL_QUARTER,
        `BNF Section Name` = SECTION_NAME,
        `BNF Section Code` = SECTION_CODE,
        `Age Band` = AGE_BAND
      ) %>%
      dplyr::summarise(
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `BNF Section Code`,
        `Age Band`
      ) %>%
      collect()
  }
  
  return(child_adult_split)
  
}


### Statistical Disclosure Control

apply_sdc <- function(data, level = 5, rounding = TRUE, round_val = 5, mask = -1) {
  
  `%>%` <- magrittr::`%>%`
  
  rnd <- round_val
  
  if(is.character(mask)) {
    type <- function(x) as.character(x)
  } else {
    type <- function(x) x
  }
  
  data %>% dplyr::mutate(
    dplyr::across(
      where(is.numeric),
      .fns = ~ dplyr::case_when(
        .x >= level & rounding == T ~ type(rnd * round(.x/rnd)),
        .x < level & .x > 0 & rounding == T ~ mask,
        .x < level & .x > 0 & rounding == F ~ mask,
        TRUE ~ type(.x)
      ),
      .names = "sdc_{.col}"
    )
  )
}

### INFO BOXES

infoBox_border <- function(
    header = "Header here",
    text = "More text here",
    backgroundColour = "#ccdff1",
    borderColour = "#005EB8",
    width = "31%",
    fontColour = "black") {
  
  #set handling for when header is blank
  display <- "block"
  
  if(header == "") {
    display <- "none"
  }
  
  paste(
    "<div class='infobox_border' style = 'border: 1px solid ", borderColour,"!important;
  border-left: 5px solid ", borderColour,"!important;
  background-color: ", backgroundColour,"!important;
  padding: 10px;
  width: ", width,"!important;
  display: inline-block;
  vertical-align: top;
  flex: 1;
  height: 100%;'>
  <h4 style = 'color: ", fontColour, ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ", display,";'>", 
    header, "</h4>
  <p style = 'color: ", fontColour, ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>", text, "</p>
</div>"
  )
}

infoBox_no_border <- function(
    header = "Header here",
    text = "More text here",
    backgroundColour = "#005EB8",
    width = "31%",
    fontColour = "white") {
  
  #set handling for when header is blank
  display <- "block"
  
  if(header == "") {
    display <- "none"
  }
  
  paste(
    "<div class='infobox_no_border',
    style = 'background-color: ",backgroundColour,
    "!important;padding: 10px;
    width: ",width,";
    display: inline-block;
    vertical-align: top;
    flex: 1;
    height: 100%;'>
  <h4 style = 'color: ", fontColour, ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ", display,";'>", 
    header, "</h4>
  <p style = 'color: ", fontColour, ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>", text, "</p>
</div>"
  )
}

