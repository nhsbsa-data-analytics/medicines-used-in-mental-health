##Functions for extracting and manipulating data from fact table in data warehouse

#ageband_extract_period() ------------------------------------------------------
#function to extract data split by 5 year agebands
#example: ageband_extract_period(con = con,
#                                 schema = schema,
#                                 table = config$sql_table_name,
#                                 period_type = "quarter")

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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
      dplyr::inner_join(dplyr::tbl(con,
                                   from = dbplyr::in_schema("DIM", "AGE_DIM")),
                        by = c("CALC_AGE" = "AGE")) %>%
      dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                                TRUE ~ DALL_5YR_BAND)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
        .groups = "drop"
      ) %>%
      dplyr::arrange(`Financial Year`,
                     `BNF Section Code`,
                     `Age Band`,
                     desc(`Identified Patient Flag`)) %>%
      collect()
  }
  
  
  else  if (period_type == "quarter") {
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
      dplyr::inner_join(dplyr::tbl(con,
                                   from = dbplyr::in_schema("DIM", "AGE_DIM")),
                        by = c("CALC_AGE" = "AGE")) %>%
      dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                                TRUE ~ DALL_5YR_BAND)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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

#age_gender_extract_period() ---------------------------------------------------
#function to extract data split by 5 year ageband and gender
#example: age_gender_extract_period(con = con,
#                                   schema = schema,
#                                   table = config$sql_table_name,
#                                   period_type = "quarter")

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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
      dplyr::inner_join(dplyr::tbl(con,
                                   from = dbplyr::in_schema("DIM", "AGE_DIM")),
                        by = c("CALC_AGE" = "AGE")) %>%
      dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                                TRUE ~ DALL_5YR_BAND)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
      ) %>%
      dplyr::arrange(`Financial Year`,
                     `BNF Section Code`,
                     `Age Band`,
                     desc(`Identified Patient Flag`)) %>%
      ungroup() %>%
      collect()
  }
  
  else  if (period_type == "quarter") {
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
      dplyr::inner_join(dplyr::tbl(con,
                                   from = dbplyr::in_schema("DIM", "AGE_DIM")),
                        by = c("CALC_AGE" = "AGE")) %>%
      dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                                TRUE ~ DALL_5YR_BAND)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
      dplyr::inner_join(dplyr::tbl(con,
                                   from = dbplyr::in_schema("DIM", "AGE_DIM")),
                        by = c("CALC_AGE" = "AGE")) %>%
      dplyr::mutate(AGE_BAND = dplyr::case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                                TRUE ~ DALL_5YR_BAND)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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

#capture_rate_extract_period() -------------------------------------------------
#function for extracting national data and calculating patient identification rate
#example: capture_rate_extract_period(con = con,
#                                     schema = schema,
#                                     table = config$sql_table_name,
#                                     period_type = "quarter")

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
      dplyr::group_by(
        FINANCIAL_YEAR,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT, na.rm = T)) %>%
      dplyr::arrange(FINANCIAL_YEAR) %>%
      collect %>%
      tidyr::pivot_wider(names_from = PATIENT_IDENTIFIED,
                         values_from = ITEM_COUNT) %>%
      mutate(
        RATE = Y / (Y + N) * 100,
        `BNF Section Code` = factor(
          `BNF Section Code`,
          levels = c("0403", "0401", "0402", "0404", "0411")
        )
      ) %>%
      dplyr::select(-Y,-N) %>%
      tidyr::pivot_wider(names_from = FINANCIAL_YEAR,
                         values_from = RATE) %>%
      dplyr::arrange(`BNF Section Code`)
  }
  else  if (period_type == "quarter") {
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::group_by(
        FINANCIAL_QUARTER,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        PATIENT_IDENTIFIED
      ) %>%
      dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT, na.rm = T)) %>%
      dplyr::arrange(FINANCIAL_QUARTER) %>%
      collect %>%
      tidyr::pivot_wider(names_from = PATIENT_IDENTIFIED,
                         values_from = ITEM_COUNT) %>%
      mutate(
        RATE = Y / (Y + N) * 100,
        `BNF Section Code` = factor(
          `BNF Section Code`,
          levels = c("0403", "0401", "0402", "0404", "0411")
        )
      ) %>%
      dplyr::select(-Y,-N) %>%
      tidyr::pivot_wider(names_from = FINANCIAL_QUARTER,
                         values_from = RATE) %>%
      dplyr::arrange(`BNF Section Code`)
  }
  
  #return data for use in pipeline
  
  return(fact)
  
}

#chem_sub_extract_period() -----------------------------------------------------
#function to extract data split by BNF chemical substance
#example: chem_sub_extract_period(con = con,
#                                 schema = schema,
#                                 table = config$sql_table_name,
#                                 period_type = "quarter")

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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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

#child_adult_extract() ---------------------------------------------------------
#function to split identified patients by under 17s and 18+
#example: child_adult_extract(con = con,
#                             schema = schema,
#                             table = config$sql_table_name,
#                             period_type = "quarter")

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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
      mutate(AGE_BAND = case_when(
        CALC_AGE < 0 ~ "Unknown",
        CALC_AGE <= 17 ~ "17 and under",
        TRUE ~ "18 and over"
      )) %>%
      dplyr::group_by(
        `Financial Year` = FINANCIAL_YEAR,
        `BNF Section Name` = SECTION_DESCR,
        `BNF Section Code` = BNF_SECTION,
        `Age Band` = AGE_BAND
      ) %>%
      dplyr::summarise(
        `Total Identified Patients` = sum(PATIENT_COUNT, na.rm = T),
        `Total Items` =  sum(ITEM_COUNT, na.rm = T),
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
        .groups = "drop"
      ) %>%
      dplyr::arrange(`Financial Year`,
                     `BNF Section Code`,
                     `Age Band`) %>%
      collect()
  }
  
  else  if (period_type == "quarter") {
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
      mutate(AGE_BAND = case_when(
        CALC_AGE < 0 ~ "Unknown",
        CALC_AGE <= 17 ~ "17 and under",
        TRUE ~ "18 and over"
      )) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
        .groups = "drop"
      ) %>%
      dplyr::arrange(`Financial Year`,
                     `Financial Quarter`,
                     `BNF Section Code`,
                     `Age Band`) %>%
      collect()
  }
  
  return(child_adult_split)
  
}

#gender_extract_period() -------------------------------------------------------
#function to split data by gender, including 'unknown' category
#example: gender_extract_period(con = con,
#                               schema = schema,
#                               table = config$sql_table_name,
#                               period_type = "quarter")

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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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

#icb_extract_period() ----------------------------------------------------------
#function to split data by integrated care board
#example: icb_extract_period(con = con,
#                             schema = schema,
#                             table = config$sql_table_name,
#                             period_type = "quarter")

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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(ICB_NAME_ORDER = case_when(`ICB Name` == "UNKNOWN ICB" ~ 2,
                                        TRUE ~ 1)) %>%
      dplyr::arrange(
        `Financial Year`,
        ICB_NAME_ORDER,
        `ICB Name`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      select(-ICB_NAME_ORDER)
  }
  else  if (period_type == "quarter") {
    #filter for year in function call
    fact <- fact_quarter %>%
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(ICB_NAME_ORDER = case_when(`ICB Name` == "UNKNOWN ICB" ~ 2,
                                        TRUE ~ 1)) %>%
      dplyr::arrange(
        `Financial Year`,
        `Financial Quarter`,
        ICB_NAME_ORDER,
        `ICB Name`,
        `BNF Section Code`,
        `BNF Paragraph Code`,
        `BNF Chemical Substance Code`,
        desc(`Identified Patient Flag`)
      ) %>%
      dplyr::select(-ICB_NAME_ORDER)
  }
  
  else  if (period_type == "month") {
    #filter for year in function call
    fact <- fact_month %>%
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
      ) %>%
      ungroup() %>%
      collect %>%
      mutate(ICB_NAME_ORDER = case_when(`ICB Name` == "UNKNOWN ICB" ~ 2,
                                        TRUE ~ 1)) %>%
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
      select(-ICB_NAME_ORDER)
  }
  
  #return data for use in pipeline
  return(fact_icb)
  
}

#imd_extract_period() ----------------------------------------------------------
#function to split data by index of multiple deprivation
#and change level from decile to quintile
#example: imd_extract_period(imd_extract_period(con = con,
#                             schema = schema,
#                             table = config$sql_table_name,
#                             period_type = "quarter"))

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
        PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
        .groups = "drop"
      ) %>%
      dplyr::arrange(`Financial Year`,
                     `BNF Section Code`,
                     `IMD Quintile`) %>%
      collect()
  }
  
  else  if (period_type == "quarter") {
    #filter for quarter in function call
    fact <- fact_quarter %>%
      dplyr::mutate(
        PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
        .groups = "drop"
      ) %>%
      dplyr::arrange(`Financial Year`,
                     `Financial Quarter`,
                     `BNF Section Code`,
                     `IMD Quintile`) %>%
      collect()
  }
  
  #return data for use in pipeline
  
  return(fact_imd)
  
}

#national_extract_period() -----------------------------------------------------
#function to get data at national level
#example: national_extract_period(con = con,
#                                 schema = schema,
#                                 table = config$sql_table_name,
#                                 period_type = "quarter")

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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
        .groups = "drop"
      ) %>%
      dplyr::arrange(`Financial Year`,
                     `BNF Section Code`,
                     desc(`Identified Patient Flag`)) %>%
      collect()
    
    return(fact_national)
    
  }
  
  else if (period_type == "quarter") {
    #filter for quarter in function call
    
    fact <- fact_quarter %>%
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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

#paragraph_extract_period() ----------------------------------------------------
#function to split data by BNF paragraph
#example: paragraph_extract_period(con = con,
#                                   schema = schema,
#                                   table = config$sql_table_name,
#                                   period_type = "quarter")

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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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
      dplyr::mutate(PATIENT_COUNT = case_when(PATIENT_IDENTIFIED == "Y" ~ 1,
                                              TRUE ~ 0)) %>%
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
        `Total Net Ingredient Cost (GBP)` = sum(ITEM_PAY_DR_NIC, na.rm = T) /
          100,
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