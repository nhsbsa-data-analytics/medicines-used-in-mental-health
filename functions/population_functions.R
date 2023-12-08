##Functions for use with population data 

#population_extract() ----------------------------------------------------------
#function to join England national mid-year population to data
#and calculate number of identified patients per 1,000 national population
#uses ons_national_pop() function from nhsbsaExternalData package
#example: population_extract()

population_extract <- function() {
  national_pop <- ons_national_pop(year = c(2015:2023),
                                   area = "ENPOP") %>%
    mutate(YEAR = as.character(YEAR))
  
  patient_population <-
    national_extract_period(con, period_type = "year") %>%
    dplyr::select(
      `Financial Year`,
      `Identified Patient Flag`,
      `Total Identified Patients`,
      `BNF Section Name`,
      `BNF Section Code`
    ) %>%
    dplyr::mutate(`Mid-year Population Year` = as.numeric(substr(c(`Financial Year`), 1, 4))) %>%
    dplyr::filter(`Identified Patient Flag` == "Y") %>%
    dplyr::left_join(select(en_ons_national_pop, YEAR, ENPOP),
                     by = c("Mid-year Population Year" = "YEAR")) %>%
    dplyr::mutate(`Patients per 1000 Population` = ((`Total Identified Patients` /
                                                       ENPOP) * 1000)) %>%
    dplyr::select(
      `Financial Year`,
      `Mid-year Population Year`,
      `BNF Section Name`,
      `BNF Section Code`,
      `Total Identified Patients`,
      `Mid-year Population Estimate` = ENPOP,
      `Patients per 1000 Population`
    ) %>%
    dplyr::arrange(`Financial Year`,
                   `BNF Section Code`)
  
  return(patient_population)
}

#national_pop_agegen() ---------------------------------------------------------
#function for use in age-gender standardised figures
#downloads and aggregate England mid-year population by age and gender
#example: national_pop_agegen()

national_pop_agegen <-
  function(url = "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales/ukpopulationestimates1838to2022/ukpopulationestimates18382022.xlsx") {
    temp1 <- tempfile()
    nat_pop_url <-
      utils::download.file(url,
                           temp1,
                           mode = "wb")
    
    #read xlsx population file for all persons for 2015 to 2022
    pop_all <- readxl::read_xlsx(temp1,
                                 sheet = 14,
                                 range = "A4:J280",
                                 col_names = TRUE) |>
      stats::na.omit() |>
      tidyr::pivot_longer(
        cols = `Mid-2022`:`Mid-2015`,
        names_to = "Year",
        values_to = "Population"
      ) |>
      dplyr::filter(`Age` != "All Ages") |>
      dplyr::mutate(`Age` = as.numeric(gsub("[^0-9.-]", "", `Age`))) |>
      dplyr::mutate(
        `Age Band` = dplyr::case_when(
          `Age` == 90 ~ "90+",
          `Age` >= 85 ~ "85-89",
          `Age` >= 80 ~ "80-84",
          `Age` >= 75 ~ "75-79",
          `Age` >= 70 ~ "70-74",
          `Age` >= 65 ~ "65-69",
          `Age` >= 60 ~ "60-64",
          `Age` >= 55 ~ "55-59",
          `Age` >= 50 ~ "50-54",
          `Age` >= 45 ~ "45-49",
          `Age` >= 40 ~ "40-44",
          `Age` >= 35 ~ "35-39",
          `Age` >= 30 ~ "30-34",
          `Age` >= 25 ~ "25-29",
          `Age` >= 20 ~ "20-24",
          `Age` >= 15 ~ "15-19",
          `Age` >= 10 ~ "10-14",
          `Age` >= 5 ~ "05-09",
          TRUE ~ "00-04"
        ),
        `Year` = dplyr::case_when(
          `Year` == "Mid-2022" ~ "2022",
          `Year` == "Mid-2021" ~ "2021",
          `Year` == "Mid-2020" ~ "2020",
          `Year` == "Mid-2019" ~ "2019",
          `Year` == "Mid-2018" ~ "2018",
          `Year` == "Mid-2017" ~ "2017",
          `Year` == "Mid-2016" ~ "2016",
          TRUE ~ "2015"
        ),
        `Sex` = dplyr::case_when(
          `Sex` == "Females" ~ "Female",
          `Sex` == "Males" ~ "Male",
          TRUE ~ "All"
        )
      ) |>
      dplyr::arrange(`Year`, `Sex`, `Age Band`) |>
      dplyr::group_by(`Year`, `Sex`, `Age Band`) |>
      dplyr::summarise(`Mid-year Population Estimate` = sum(`Population`),
                       .groups = "drop")
    
    return(pop_all)
  }