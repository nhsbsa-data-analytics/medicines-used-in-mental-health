### data warehouse extracts from fact table

capture_rate_extract_period <- function(con,
                                        schema,
                                        table,
                                        period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))   
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

national_extract_period <- function(con,
                                    schema,
                                    table,
                                    period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))
  fact_month <- dplyr::tbl(src = con,
                           dbplyr::in_schema(schema, table))
  
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
  national_pop <- ons_national_pop(year = c(2015:2023),
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

paragraph_extract_period <- function(con,
                                     schema,
                                     table,
                                     period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))
  fact_month <- dplyr::tbl(src = con,
                           dbplyr::in_schema(schema, table))
  
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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

chem_sub_extract_period <- function(con,
                                    schema,
                                    table,
                                    period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))
  fact_month <- dplyr::tbl(src = con,
                           dbplyr::in_schema(schema, table))
  
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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

icb_extract_period <- function(con,
                               schema,
                               table,
                               period_type = c("year", "quarter", "month")) {
  
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))
  fact_month <- dplyr::tbl(src = con,
                           dbplyr::in_schema(schema, table))
  
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(
        ICB_NAME_ORDER = case_when(
          `ICB Name` == "UNKNOWN ICB" ~ 2,
          TRUE ~ 1
        )
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        ICB_NAME_ORDER,
        `ICB Name`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      select(
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(
        ICB_NAME_ORDER = case_when(
          `ICB Name` == "UNKNOWN ICB" ~ 2,
          TRUE ~ 1
        )
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        ICB_NAME_ORDER,
        `ICB Name`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Identified Patient Flag`)) %>%
      dplyr::select(
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(
        ICB_NAME_ORDER = case_when(
          `ICB Name` == "UNKNOWN ICB" ~ 2,
          TRUE ~ 1
        )
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Finacial Quarter`,
        `Year Month`,
        ICB_NAME_ORDER,
        `ICB Name`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      select(
        -ICB_NAME_ORDER
      )
  }
  
  #return data for use in pipeline
  return(fact_icb)
  
}

ageband_extract_period <- function(con,
                                   schema,
                                   table,
                                   period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))   
  
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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

gender_extract_period <- function(con,
                                  schema,
                                  table,
                                  period_type = c("year", "quarter")) {
  
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))   
  
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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

age_gender_extract_period <- function(con,
                                      schema,
                                      table,
                                      period_type = c("year", "quarter", "month")) {
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))   
  fact_month <- dplyr::tbl(src = con,
                           dbplyr::in_schema(schema, table)) 
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
  
  else  if (period_type == "month") {
    
    #filter for month in function call
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
        `Year Month` = YEAR_MONTH,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Age Band` = AGE_BAND,
        `Patient Gender` = GENDER_DESCR,
        `Identified Patient Flag` = PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `Year Month`,
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

imd_extract_period <- function(con,
                               schema,
                               table,
                               period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))   
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
            IMD_DECILE %in% c("9", "10") ~ "5 - Least Deprived",
            TRUE ~ "Unknown"
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
            IMD_DECILE %in% c("9", "10") ~ "5 - Least Deprived",
            TRUE ~ "Unknown"
          )
        
      ) %>%
      dplyr::group_by(
        FINANCIAL_YEAR,
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
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` = sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
        .groups = "drop"
      ) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        `BNF Section Code`,
        `IMD Quintile`
      ) %>%
      collect()
  }
  
  #return data for use in pipeline  
  
  return(fact_imd)
  
}

child_adult_extract <- function(con,
                                schema,
                                table,
                                period_type = c("year", "quarter")) {
  fact_year <- dplyr::tbl(src = con,
                          dbplyr::in_schema(schema, table))
  fact_quarter <- dplyr::tbl(src = con,
                             dbplyr::in_schema(schema, table))   
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
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Age Band` = AGE_BAND
      ) %>%
      dplyr::summarise(
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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
        SECTION_DESCR,
        BNF_SECTION,
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
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Age Band` = AGE_BAND
      ) %>%
      dplyr::summarise(
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T)/100,
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

apply_sdc <-
  function(data,
           level = 5,
           rounding = TRUE,
           round_val = 5,
           mask = NA) {
    `%>%` <- magrittr::`%>%`
    
    rnd <- round_val
    
    if (is.character(mask)) {
      type <- function(x)
        as.character(x)
    } else {
      type <- function(x)
        x
    }
    
    data %>% dplyr::mutate(dplyr::across(
      where(is.numeric),
      .fns = ~ dplyr::case_when(
        .x >= level & rounding == T ~ type(rnd * round(.x / rnd)),
        .x < level & .x > 0 & rounding == T ~ mask,
        .x < level & .x > 0 & rounding == F ~ mask,
        TRUE ~ type(.x)
      ),
      .names = "sdc_{.col}"
    ))
  }

### Covid model functions

ageband_manip_20yr <- function(data) {
  #aggregate from 5 year ageband to 20 year ageband
  data_20yr <- data %>%
    dplyr::mutate(
      BAND_20YR = dplyr::case_when(
        `Age Band` %in% c("00-04", "05-09", "10-14", "15-19") ~ "00-19",
        `Age Band` %in% c("20-24", "25-29", "30-34", "35-39") ~ "20-39",
        `Age Band` %in% c("40-44", "45-49", "50-54", "55-59") ~ "40-59",
        `Age Band` %in% c("60-64", "65-69", "70-74", "75-79") ~ "60-79",
        `Age Band` == "Unknown" ~ "Unknown",
        TRUE ~ "80+"
      )
    ) %>%
    dplyr::select(!(`Age Band`)) %>%
    dplyr::group_by(
      YEAR_MONTH = `Year Month`,
      SECTION_NAME = `BNF Section Name`,
      SECTION_CODE = `BNF Section Code`,
      IDENTIFIED_FLAG = `Identified Patient Flag`,
      PDS_GENDER = `Patient Gender`,
      BAND_20YR
    ) %>%
    dplyr::summarise(
      ITEM_COUNT = sum(`Total Items`),
      ITEM_PAY_DR_NIC = sum(`Total Net Ingredient Cost (GBP)`),
      .groups = "drop"
    ) %>%
    #fill in ageband and gender levels with 0 items
    tidyr::complete(
      BAND_20YR,
      nesting(
        YEAR_MONTH,
        SECTION_NAME,
        SECTION_CODE,
        IDENTIFIED_FLAG,
        PDS_GENDER
      ),
      fill = list(
        ITEM_COUNT = 0,
        ITEM_PAY_DR_NIC = 0,
        PATIENT_COUNT = 0
      )
    ) %>%
    tidyr::complete(
      IDENTIFIED_FLAG,
      nesting(
        YEAR_MONTH,
        SECTION_NAME,
        SECTION_CODE,
        BAND_20YR,
        PDS_GENDER
      ),
      fill = list(
        ITEM_COUNT = 0,
        ITEM_PAY_DR_NIC = 0,
        PATIENT_COUNT = 0
      )
    ) %>%
    tidyr::complete(
      PDS_GENDER,
      nesting(
        YEAR_MONTH,
        SECTION_NAME,
        SECTION_CODE,
        IDENTIFIED_FLAG,
        BAND_20YR
      ),
      fill = list(
        ITEM_COUNT = 0,
        ITEM_PAY_DR_NIC = 0,
        PATIENT_COUNT = 0
      )
    ) %>%
    dplyr::group_by(SECTION_NAME,
                    SECTION_CODE,
                    IDENTIFIED_FLAG,
                    PDS_GENDER,
                    BAND_20YR) %>%
    dplyr::group_by(SECTION_NAME,
                    SECTION_CODE,
                    IDENTIFIED_FLAG,
                    PDS_GENDER,
                    BAND_20YR) %>%
    #get month place in year, month since start of data, month placement
    #mutate to add as columns for use in later functions
    dplyr::mutate(
      MONTH_START = as.Date(paste0(YEAR_MONTH, "01"), format = "%Y%m%d"),
      MONTH_NUM = lubridate::month(MONTH_START),
      MONTH_INDEX = lubridate::interval(lubridate::dmy(01032015), as.Date(MONTH_START)) %/% months(1)
    ) %>%
    #join dispensing days for each month
    #remove patients with unknown ageband or gender
    dplyr::left_join(dispensing_days_data,
                     by = "YEAR_MONTH") %>%
    dplyr::filter(
      !(IDENTIFIED_FLAG == "N" & PDS_GENDER == "F"),!(IDENTIFIED_FLAG == "N" &
                                                        PDS_GENDER == "M"),!(PDS_GENDER == "U" |
                                                                               BAND_20YR == "Unknown")
    ) %>%
    dplyr::ungroup() %>%
    #turn each level of MONTH_NUM column into 12 separate columns
    #for use as variables of month of the year in linear model
    dplyr::mutate(
      m_01 = 1 * (MONTH_NUM == 1),
      m_02 = 1 * (MONTH_NUM == 2),
      m_03 = 1 * (MONTH_NUM == 3),
      m_04 = 1 * (MONTH_NUM == 4),
      m_05 = 1 * (MONTH_NUM == 5),
      m_06 = 1 * (MONTH_NUM == 6),
      m_07 = 1 * (MONTH_NUM == 7),
      m_08 = 1 * (MONTH_NUM == 8),
      m_09 = 1 * (MONTH_NUM == 9),
      m_10 = 1 * (MONTH_NUM == 10),
      m_11 = 1 * (MONTH_NUM == 11),
      m_12 = 1 * (MONTH_NUM == 12)
    ) %>%
    ungroup() %>%
    #add column to show if month pre-covid or covid period onwards
    dplyr::mutate(time_period = case_when(YEAR_MONTH <= 202002 ~ "pre_covid",
                                          TRUE ~ "covid"))
  return(data_20yr)
}

covid_lm <- function(training_data,
                     section_code) {
  #create linear model using lm() function with all required variables
  #include age and gender as interaction term
  #include month position within year as separate columns
  covid_lm_with_months <-
    lm(
      ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + m_02 + m_03
      + m_04 + m_05 + m_06 + m_07 + m_08 + m_09 + m_10 + m_11 + m_12
      + as.factor(PDS_GENDER) * as.factor(BAND_20YR),
      data = filter(training_data, SECTION_CODE == section_code)
    )
  
  return(covid_lm_with_months)
  
}

fast_agg_pred <- function (w, lmObject, newdata, alpha = 0.95) {
  ## input checking
  
  if (!inherits(lmObject, "lm"))
    stop("'lmObject' is not a valid 'lm' object!")
  if (!is.data.frame(newdata))
    newdata <- as.data.frame(newdata)
  if (length(w) != nrow(newdata))
    stop("length(w) does not match nrow(newdata)")
  
  ## extract "terms" object from the fitted model, but delete response variable
  tm <- delete.response(terms(lmObject))
  
  ## linear predictor matrix
  Xp <- model.matrix(tm, newdata)
  
  ## predicted values by direct matrix-vector multiplication
  pred <- c(Xp %*% coef(lmObject))
  
  ## mean of the aggregation
  agg_mean <- c(crossprod(pred, w))
  
  ## residual variance
  sig2 <- c(crossprod(residuals(lmObject))) / df.residual(lmObject)
  
  ## efficiently compute variance of the aggregation without matrix-matrix computations
  
  QR <- lmObject$qr   ## qr object of fitted model
  piv <- QR$pivot     ## pivoting index
  r <- QR$rank        ## model rank / numeric rank
  
  u <- forwardsolve(t(QR$qr), c(crossprod(Xp, w))[piv], r)
  
  agg_variance <- c(crossprod(u)) * sig2
  
  ## adjusted variance of the aggregation
  agg_variance_adj <- agg_variance + c(crossprod(w)) * sig2
  
  ## t-distribution quantiles
  Qt <-
    c(-1, 1) * qt((1 - alpha) / 2, lmObject$df.residual, lower.tail = FALSE)
  
  ## names of CI and PI
  NAME <- c("lower", "upper")
  
  ## CI
  CI <- setNames(agg_mean + Qt * sqrt(agg_variance), NAME)
  
  ## PI
  PI <- setNames(agg_mean + Qt * sqrt(agg_variance_adj), NAME)
  
  ## return
  list(
    mean = agg_mean,
    var = agg_variance,
    CI = CI,
    PI = PI
  )
}

month_pred_fun <- function(month, data, model, alpha = 0.95) {
  
  data <- data %>%
    dplyr::filter(YEAR_MONTH == month)
  
  pred <-
    fast_agg_pred(rep.int(1, nrow(data)),
                  data,
                  lmObject = model,
                  alpha = alpha)
  pred99 <-
    fast_agg_pred(rep.int(1, nrow(data)),
                  data,
                  lmObject = model,
                  alpha = 0.99)
  output <- data.frame(UNIT = 1)
  
  #use list of months within data
  #include columns for lower and upper 95% and 99% CI
  output$YEAR_MONTH <- month
  output$MEAN_FIT <- pred[["mean"]]
  output$VAR <- pred[["var"]]
  output$PILWR <- pred[["PI"]][["lower"]]
  output$PIUPR <- pred[["PI"]][["upper"]]
  output$PILWR99 <- pred99[["PI"]][["lower"]]
  output$PIUPR99 <- pred99[["PI"]][["upper"]]
  output$UNIT <- NULL
  
  return(output)
  
}

prediction_list <- function(data,
                            section_code,
                            covid_lm_output,
                            pred_month_list) {
  pred_month_list <- df20 %>%
    dplyr::filter(YEAR_MONTH >= 202003) %>%
    pull(YEAR_MONTH) %>%
    unique()
  
  #get predictions based on BNF section code
  if (section_code == "0401") {
    df_0401 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #apply month_pred_fun() on each month within pred_month_list
    pred_0401 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0401,
                        model = covid_lm_output)
    
    unlist(pred_0401)
    
    rbindlist(pred_0401)
    
    #create final dataset by binding predictions onto actual items data
    #add YEAR_MONTH as character column for easier use in chart
    section_pred_list <- df_0401 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0401)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  else if (section_code == "0402") {
    df_0402 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #default PI of 95%
    pred_0402 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0402,
                        model = covid_lm_output)
    
    unlist(pred_0402)
    
    rbindlist(pred_0402)
    
    section_pred_list <- df_0402 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0402)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  else if (section_code == "0403") {
    df_0403 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #default PI of 95%
    pred_0403 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0403,
                        model = covid_lm_output)
    
    unlist(pred_0403)
    
    rbindlist(pred_0403)
    
    section_pred_list <- df_0403 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0403)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  else if (section_code == "0404") {
    df_0404 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #default PI of 95%
    pred_0404 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0404,
                        model = covid_lm_output)
    
    unlist(pred_0404)
    
    rbindlist(pred_0404)
    
    section_pred_list <- df_0404 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0404)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  else if (section_code == "0411") {
    df_0411 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #default PI of 95%
    pred_0411 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0411,
                        model = covid_lm_output)
    
    unlist(pred_0411)
    
    rbindlist(pred_0411)
    
    section_pred_list <- df_0411 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0411)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  return(section_pred_list)
  
}

# Info boxes -------------------------------------------------------------
infoBox_border <- function(header = "Header here",
                           text = "More text here",
                           backgroundColour = "#ccdff1",
                           borderColour = "#005EB8",
                           width = "31%",
                           fontColour = "black") {
  #set handling for when header is blank
  display <- "block"
  
  if (header == "") {
    display <- "none"
  }
  
  paste(
    "<div class='infobox_border' style = 'border: 1px solid ",
    borderColour,
    "!important;
  border-left: 5px solid ",
  borderColour,
  "!important;
  background-color: ",
  backgroundColour,
  "!important;
  padding: 10px;
  width: ",
  width,
  "!important;
  display: inline-block;
  vertical-align: top;
  flex: 1;
  height: 100%;'>
  <h4 style = 'color: ",
  fontColour,
  ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ",
  display,
  ";'>",
  header,
  "</h4>
  <p style = 'color: ",
  fontColour,
  ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>",
  text,
  "</p>
</div>"
  )
}

infoBox_no_border <- function(header = "Header here",
                              text = "More text here",
                              backgroundColour = "#005EB8",
                              width = "31%",
                              fontColour = "white") {
  #set handling for when header is blank
  display <- "block"
  
  if (header == "") {
    display <- "none"
  }
  
  paste(
    "<div class='infobox_no_border',
    style = 'background-color: ",
    backgroundColour,
    "!important;padding: 10px;
    width: ",
    width,
    ";
    display: inline-block;
    vertical-align: top;
    flex: 1;
    height: 100%;'>
  <h4 style = 'color: ",
  fontColour,
  ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ",
  display,
  ";'>",
  header,
  "</h4>
  <p style = 'color: ",
  fontColour,
  ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>",
  text,
  "</p>
</div>"
  )
}
### Chart functions
age_gender_chart <- function(data,
                             labels = FALSE) {
  age_gender_chart_data <- data |>
    dplyr::select(`Age Band`,
                  `Patient Gender`,
                  `Total Identified Patients`) |>
    tidyr::complete(`Patient Gender`,
                    `Age Band`,
                    fill = list(`Total Identified Patients` = 0))
  
  categories = c(unique(age_gender_chart_data$`Age Band`))
  
  max <- max(age_gender_chart_data$`Total Identified Patients`)
  min <- max(age_gender_chart_data$`Total Identified Patients`) * -1
  
  male <- age_gender_chart_data |>
    dplyr::filter(`Patient Gender` == "Male")
  
  female <- age_gender_chart_data |>
    dplyr::filter(`Patient Gender` == "Female") |>
    dplyr::mutate(`Total Identified Patients` = 0 - `Total Identified Patients`)
  
  hc <- highcharter::highchart() |>
    highcharter::hc_chart(type = 'bar') |>
    hc_chart(style = list(fontFamily = "Arial")) |>
    highcharter::hc_xAxis(
      list(
        title = list(text = "Age group"),
        categories = categories,
        reversed = FALSE,
        labels = list(step = 1)
      ),
      list(
        categories = categories,
        opposite = TRUE,
        reversed = FALSE,
        linkedTo = 0,
        labels = list(step = 1)
      )
    ) |>
    highcharter::hc_tooltip(
      shared = FALSE,
      formatter = JS(
        "function () {
                   return this.point.category + '<br/>' +
                   '<b>' + this.series.name + '</b> ' +
                   Highcharts.numberFormat(Math.abs(this.point.y), 0);}"
      )
    ) |>
    highcharter::hc_yAxis(
      title = list(text = "Identified patients"),
      max = max,
      min = min,
      labels = list(
        formatter = JS(
          'function () {
               result = Math.abs(this.value);
               if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
               return result;
             }'
        )
      )
    ) |>
    highcharter::hc_plotOptions(series = list(stacking = 'normal')) |>
    highcharter::hc_series(
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#8e5300',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#8e5300",
        fontFamily = "Ariel",
        name = 'Male',
        data = c(male$`Total Identified Patients`)
      ),
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#003087',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y * -1;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return result.toPrecision(3) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return result.toPrecision(3) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#003087",
        name = 'Female',
        fontFamily = "Ariel",
        data = c(female$`Total Identified Patients`)
      )
    ) |>
    highcharter::hc_legend(reversed = T) |>
    highcharter::hc_credits(enabled = TRUE)
  
  return(hc)
  
}

age_gender_chart_no_fill <- function(data,
                                     labels = FALSE) {
  age_gender_chart_data <- data %>%
    dplyr::select(`Age Band`,
                  `Patient Gender`,
                  `Total Identified Patients`)
  
  categories = c(unique(age_gender_chart_data$`Age Band`))
  
  max <- max(age_gender_chart_data$`Total Identified Patients`)
  min <- max(age_gender_chart_data$`Total Identified Patients`) * -1
  
  male <- age_gender_chart_data %>%
    dplyr::filter(`Patient Gender` == "Male")
  
  female <- age_gender_chart_data %>%
    dplyr::filter(`Patient Gender` == "Female") %>%
    dplyr::mutate(`Total Identified Patients` = 0 - `Total Identified Patients`)
  
  hc <- highcharter::highchart() %>%
    highcharter::hc_chart(type = 'bar') %>%
    hc_chart(style = list(fontFamily = "Arial")) %>%
    highcharter::hc_xAxis(
      list(
        title = list(text = "Age group"),
        categories = categories,
        reversed = FALSE,
        labels = list(step = 1)
      ),
      list(
        categories = categories,
        opposite = TRUE,
        reversed = FALSE,
        linkedTo = 0,
        labels = list(step = 1)
      )
    ) %>%
    highcharter::hc_tooltip(
      shared = FALSE,
      formatter = JS(
        "function () {
                   return this.point.category + '<br/>' +
                   '<b>' + this.series.name + '</b> ' +
                   Highcharts.numberFormat(Math.abs(this.point.y), 0);}"
      )
    ) %>%
    highcharter::hc_yAxis(
      title = list(text = "Identified patients"),
      max = max,
      min = min,
      labels = list(
        formatter = JS(
          'function () {
               result = Math.abs(this.value);
               if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
               return result;
             }'
        )
      )
    ) %>%
    highcharter::hc_plotOptions(series = list(stacking = 'normal')) %>%
    highcharter::hc_series(
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#8e5300',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#8e5300",
        fontFamily = "Ariel",
        name = 'Male',
        data = c(male$`Total Identified Patients`)
      ),
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#003087',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y * -1;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return result.toPrecision(3) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return result.toPrecision(3) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#003087",
        name = 'Female',
        fontFamily = "Ariel",
        data = c(female$`Total Identified Patients`)
      )
    ) %>%
    highcharter::hc_legend(reversed = T)
  
  return(hc)
  
}

covid_chart_hc <- function(data,
                           title = NULL) {
  chart_data <- data %>%
    dplyr::mutate(
      ACT = prettyNum(signif(TOTAL_ITEMS, 3), big.mark = ","),
      EXP = prettyNum(signif(MEAN_FIT, 3), big.mark = ","),
      RANGE_95 = paste(
        formatC(signif(PILWR, 3), big.mark = ",", format = "f", digits = 0),
        "-",
        formatC(signif(PIUPR, 3), big.mark = ",", format = "f", digits = 0)
      ),
      RANGE_99 = paste(
        formatC(signif(PILWR99, 3), big.mark = ",", format = "f", digits = 0),
        "-",
        formatC(signif(PIUPR99, 3), big.mark = ",", format = "f", digits = 0)
      ),
      MONTH_START = as.Date(paste0(YEAR_MONTH_STRING, "01"), format = "%Y%m%d")
    )
  
  
  chart <- highchart() %>%
    highcharter::hc_chart(style = list(fontFamily = "Arial")) %>%
    highcharter::hc_add_series(
      data = chart_data,
      name = "99% prediction interval",
      type = "arearange",
      lineWidth = 0,
      color = "#768692",
      marker = list(enabled = FALSE),
      dataLabels = list(enabled = FALSE),
      # enableMouseTracking = FALSE,
      highcharter::hcaes(
        x = MONTH_START,
        high = signif(PIUPR99, 3),
        low = signif(PILWR99, 3),
        tooltip = RANGE_99
      )
    ) %>%
    #highcharter::hc_add_series(
    #data = chart_data,
    #name = "95% prediction interval",
    #type = "arearange",
    #lineWidth = 0,
    #color = "#b3bbc1",
    #marker = list(enabled = FALSE),
    #dataLabels = list(enabled = FALSE),
    #hcaes(
    #x = MONTH_START,
    #high = signif(PIupr, 3),
  #low = signif(PIlwr, 3),
  #tooltip = RANGE_95
  #)
  #) %>%
  highcharter::hc_add_series(
    data = chart_data,
    name = "Expected items",
    type = "line",
    dashStyle = "Dash",
    color = "#231f20",
    marker = list(enabled = FALSE),
    dataLabels = list(enabled = FALSE),
    hcaes(
      x = MONTH_START,
      y = signif(MEAN_FIT, 3),
      tooltip = EXP
    )
  ) %>%
    highcharter::hc_add_series(
      data = chart_data,
      name = "Prescribed items",
      type = "line",
      lineWidth = 3,
      color = "#003087",
      marker = list(enabled = FALSE),
      dataLabels = list(enabled = FALSE),
      hcaes(
        x = MONTH_START,
        y = signif(TOTAL_ITEMS, 3),
        tooltip = ACT
      )
    ) %>%
    highcharter::hc_xAxis(
      type = "datetime",
      dateTimeLabelFormats = list(month = "%b %y"),
      title = list(text = "Month")
    ) %>%
    highcharter::hc_yAxis(title = list(text = "Volume"),
                          min = 0) %>%
    highcharter::hc_title(text = title,
                          style = list(fontSize = "16px",
                                       fontWeight = "bold")) %>%
    highcharter::hc_legend(enabled = TRUE,
                           reversed = TRUE) %>%
    highcharter::hc_tooltip(
      enabled = TRUE,
      shared = TRUE,
      useHTML = TRUE,
      formatter = JS(
        "function () {
        var timeStamp = this.x;
        var dateFormat = new Date(timeStamp);
        var month = dateFormat.toLocaleString('default', { month: 'long' });
        var year = dateFormat.getFullYear();

        var s = month + ' ' + year;

        $.each(this.points.reverse(), function () {
            var number = this.point.tooltip;

            s += '<br/><span style=\"color:' + this.series.color + '\">\u25CF</span> ' + this.series.name + ': ' +
                '<b>' + number + '</b>';
        });

        return s;
    }"
      )
    ) %>%
    highcharter::hc_credits(enabled = TRUE) %>%
    highcharter::hc_plotOptions(arearange = list(states = list(hover = list(enabled = FALSE))))
  
  
  # explicit return
  return(chart)
  
}

group_chart_hc_new <- function(data, 
                               x,
                               y,
                               type = "line",
                               group,
                               xLab = NULL,
                               yLab = NULL,
                               title = NULL,
                               dlOn = TRUE,
                               currency = FALSE,
                               marker = TRUE) {
  # this function creates a group bar chart with NHSBSA data vis standards
  # applied. includes datalabel formatter to include "£" if needed.
  
  x <- rlang::enexpr(x)
  y <- rlang::enexpr(y)
  
  group <- rlang::enexpr(group)
  
  # set font to arial
  font <- "Arial"
  
  # get number of groups. max number of groups is 9 for unique colors
  num_groups <- length(unique(data[[group]]))
  
  # define a set of colors
  colors <- c("#03045E", "#0077B6", "#00B4D8", "#90E0EF", "#CAF0F8")
  
  # if there are more groups than colors, recycle the colors
  if (num_groups > length(colors)) {
    colors <- rep(colors, length.out = num_groups)
  }
  
  
  #if there is a 'Total' groups ensure this takes the color black
  if ("Total" %in% unique(data[[group]])) {
    
    #identify index of "total" group
    total_index <- which(sort(unique(data[[group]])) == "Total")
    
    # add black to location of total_index
    colors <- c(colors[1:total_index-1], "#000000", colors[total_index:length(colors)])
  }
  
  # subset the colors to the number of groups
  #colors <- ifelse(unique(data[[group]]) == "Total", "black", colors[1:num_groups])
  
  # check currency argument to set symbol
  dlFormatter <- highcharter::JS(
    paste0("function() {
    var ynum = this.point.y;
    var options = { maximumSignificantDigits: 3, minimumSignificantDigits: 3 };
      if (",tolower(as.character(currency)),") {
      options.style = 'currency';
      options.currency = 'GBP';
      }
      if (ynum >= 1000000000) {
        options.maximumSignificantDigits = 4;
        options.minimumSignificantDigits = 4;
      }else {
       options.maximumSignificantDigits = 3;
        options.minimumSignificantDigits = 3;
      }
    return ynum.toLocaleString('en-GB', options);
  }")
  )
  
  
  # ifelse(is.na(str_extract(!!y, "(?<=\\().*(?=,)")),!!y,str_extract(!!y, "(?<=\\().*(?=,)")),
  
  # check chart type to set grid lines
  gridlineColor <- if (type == "line") "#e6e6e6" else "transparent"
  
  # check chart type to turn on y axis labels
  yLabels <- if (type == "line") TRUE else FALSE
  
  # highchart creation
  chart <- highcharter::highchart() |>
    highcharter::hc_chart(style = list(fontFamily = font)) |>
    highcharter::hc_colors(colors) |>
    # add only series
    highcharter::hc_add_series(
      data = data,
      type = type,
      marker = list(enabled = marker),
      highcharter::hcaes(
        x = !!x,
        y = !!y,
        group = !!group
      ),
      groupPadding = 0.1,
      pointPadding = 0.05,
      dataLabels = list(
        enabled = dlOn,
        formatter = dlFormatter,
        style = list(textOutline = "none")
      )
    ) |>
    highcharter::hc_xAxis(type = "category",
                          title = list(text = xLab)) |>
    # turn off y axis and grid lines
    highcharter::hc_yAxis(
      title = list(text = yLab),
      labels = list(enabled = yLabels),
      gridLineColor = gridlineColor,
      min = 0
    ) |>
    highcharter::hc_title(text = title,
                          style = list(fontSize = "16px",
                                       fontWeight = "bold")) |>
    highcharter::hc_legend(enabled = TRUE) |>
    highcharter::hc_tooltip(enabled = FALSE) |>
    highcharter::hc_credits(enabled = TRUE)
  
  # explicit return
  return(chart)
}

### CSV Download button
get_download_button <- function(data = data, title = "Download chart data", filename = "data") { 
  dt <- datatable(data, rownames = FALSE,
                  extensions = 'Buttons',
                  options = list(
                    searching = FALSE,
                    paging = TRUE,
                    bInfo = FALSE,
                    pageLength = 1,
                    dom = '<"datatable-wrapper"B>',
                    buttons = list(
                      list(extend = 'csv',
                           text = title,
                           filename = filename,
                           className = "nhs-button-style")
                    ),
                    initComplete = JS(
                      "function(settings, json) {",
                      "$(this.api().table().node()).css('visibility', 'collapse');",
                      "}"
                    )
                  )
  )
  
  return(dt)
}