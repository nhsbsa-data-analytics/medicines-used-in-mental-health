# pipeline_quarterly.R
# this script provides the code to run the reproducible analytical pipeline
# and produce the Medicines Used in Mental Health (MUMH) quarterly publication

# contains the following sections:
# 1. Setup and package installation
# 2. Data import
# 3. Aggregations and analysis
# 4. Model of pre-COVID-19-pandemic prescribing trends
# 5. Data tables
# 6. Charts and figures
# 7. Render outputs

# clear environment
rm(list = ls())

# source functions
# select all .R files in functions sub-folder
function_files <- list.files(path = "functions", pattern = "\\.R$")

# loop over function_files to source all files in functions sub-folder
for (file in function_files) {
  source(file.path("functions", file))
}

#1. Setup and package installation ---------------------------------------------

# load GITHUB_KEY if available in environment or enter if not
if (Sys.getenv("GITHUB_PAT") == "") {
  usethis::edit_r_environ()
  stop(
    "You need to set your GITHUB_PAT = YOUR PAT KEY in the .Renviron file which pops up. Please restart your R Studio after this and re-run the pipeline."
  )
}

# load database credentials if available in environment or enter if not
if (Sys.getenv("DB_DWCP_USERNAME") == "") {
  usethis::edit_r_environ()
  stop(
    "You need to set your DB_DWCP_USERNAME = YOUR DWCP USERNAME and  DB_DWCP_PASSWORD = YOUR DWCP PASSWORD in the .Renviron file which pops up. Please restart your R Studio after this and re-run the pipeline."
  )
}

# check if Excel outputs are required
makeSheet <- menu(c("Yes", "No"),
                  title = "Do you wish to generate the Excel outputs?")

# install and load devtools package
install.packages("devtools")
library(devtools)

# install nhsbsaUtils package first to use function check_and_install_packages()
devtools::install_github(
  "nhsbsa-data-analytics/nhsbsaUtils",
  auth_token = Sys.getenv("GITHUB_PAT"),
  force = TRUE
)

library(nhsbsaUtils)

# install required packages
# double check required packages once full pipeline built eg. if maps used
req_pkgs <- c(
  "broom",
  "data.table",
  "devtools",
  "DBI",
  "dbplyr",
  "dplyr",
  "DT" ,
  "geojsonsf",
  "highcharter",
  "htmltools",
  "janitor",
  "kableExtra",
  "lubridate",
  "logr",
  "magrittr",
  "nhsbsa-data-analytics/nhsbsaR",
  "nhsbsa-data-analytics/nhsbsaExternalData",
  "nhsbsa-data-analytics/accessibleTables",
  "nhsbsa-data-analytics/nhsbsaDataExtract",
  "nhsbsa-data-analytics/nhsbsaVis",
  "openxlsx",
  "rmarkdown",
  "rsample",
  "sf",
  "stringr",
  "svDialogs",
  "tcltk",
  "tidyr",
  "tidyverse",
  "vroom",
  "yaml"
)

# library/install packages as required
nhsbsaUtils::check_and_install_packages(req_pkgs)

# set up logging
lf <-
  logr::log_open(paste0(
    "Y:/Official Stats/MUMH/log/mumh_log",
    format(Sys.time(), "%d%m%y%H%M%S"),
    ".log"
  ))

# load config
config <- yaml::yaml.load_file("config.yml")
log_print("Config loaded", hide_notes = TRUE)
log_print(config, hide_notes = TRUE)

# load options
nhsbsaUtils::publication_options()
log_print("Options loaded", hide_notes = TRUE)

#2. Data import ----------------------------------------------------------------

con <- nhsbsaR::con_nhsbsa(dsn = "FBS_8192k",
                           driver = "Oracle in OraClient19Home1",
                           "DWCP")

schema <-
  as.character(svDialogs::dlgInput("Enter schema name: ")$res)

# quarterly data extracts

capture_rate_extract_quarter <-
  capture_rate_extract_period(
    con = con,
    schema = schema,
    table = config$sql_table_name,
    period_type = "quarter"
  )
national_extract_quarter <- national_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "quarter"
)
paragraph_extract_quarter <- paragraph_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "quarter"
)
chem_sub_extract_quarter <- chem_sub_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "quarter"
)
icb_extract_quarter <- icb_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "quarter"
)
ageband_data_quarter <- ageband_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "quarter"
)
gender_extract_quarter <- gender_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "quarter"
)
age_gender_extract_quarter <- age_gender_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "quarter"
)
imd_extract_quarter <- imd_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "quarter"
)
child_adult_extract_quarter <- child_adult_extract(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "quarter"
)

log_print("Quarterly extracts pulled", hide_notes = TRUE)

# monthly data extracts

national_extract_monthly <- national_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "month"
)
paragraph_extract_monthly <- paragraph_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "month"
)
chem_sub_extract_monthly <- chem_sub_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "month"
)
age_gender_extract_month <- age_gender_extract_period(
  con = con,
  schema = schema,
  table = config$sql_table_name,
  period_type = "month"
)

log_print("Monthly extracts pulled", hide_notes = TRUE)

# disconnect from data warehouse once all extracts pulled

DBI::dbDisconnect(con)

# external data extracts

# mid-year England population by ageband and gender 2015 to 2022
pop_agegen_2022 <- national_pop_agegen() |>
  dplyr::mutate(`Year` = as.double(`Year`)) |>
  dplyr::filter(`Sex` != "All")

# national mid-year England population 2022
total_pop_eng_2022 <- pop_agegen_2022 |>
  dplyr::filter(`Sex` == "All",
                `Year` == "2022") |>
  dplyr::summarise(`Total population` = sum(`Mid-year Population Estimate`))

log_print("External data pulled", hide_notes = TRUE)

#3. Aggregations and analysis --------------------------------------------------

# 0401 Hypnotics and anxiolytics workbook - quarterly

quarterly_0401 <- list()

quarterly_0401$patient_id <- capture_rate_extract_quarter |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$national_total <- national_extract_quarter |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$national_paragraph <- paragraph_extract_quarter |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$icb <-  icb_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `ICB Name`,
    `ICB Code`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$gender <- gender_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$ageband <- ageband_data_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$age_gender <- age_gender_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$imd <- imd_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$monthly_section <- national_extract_monthly |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Identified Patient Flag`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$monthly_paragraph <- paragraph_extract_monthly |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$monthly_chem_substance <- chem_sub_extract_monthly |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$avg_per_pat <- chem_sub_extract_monthly |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401",
                `Identified Patient Flag` == "Y") |>
  dplyr::mutate(
    `Average Items per patient` = (`Total Items` / `Total Identified Patients`),
    `Average NIC per Patient (GBP)` = (`Total Net Ingredient Cost (GBP)` /
                                         `Total Identified Patients`)
  )

quarterly_0401$pat_per_1000_pop <- age_gender_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::ungroup() |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401") |>
  dplyr::mutate(`Mid-year Population Year` = as.numeric((substr(
    c(`Financial Year`), 1, 4
  ))), .after = `Financial Year`) |>
  dplyr::filter(`Identified Patient Flag` == "Y",
                `Age Band` != "Unknown") |>
  stats::na.omit() |>
  dplyr::left_join(
    select(
      pop_agegen_2022,
      `Year`,
      `Sex`,
      `Age Band`,
      `Mid-year Population Estimate`
    ),
    by = c(
      "Mid-year Population Year" = "Year",
      "Patient Gender" = "Sex",
      "Age Band" = "Age Band"
    )
  ) |>
  dplyr::mutate(`Patients per 1,000 Population` = ((`Total Identified Patients` /
                                                      `Mid-year Population Estimate`) * 1000
  ))

# 0402 Drugs used in psychoses and related disorders workbook - quarterly

quarterly_0402 <- list()

quarterly_0402$patient_id <- capture_rate_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$national_total <- national_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$national_paragraph <- paragraph_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$icb <-  icb_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `ICB Name`,
    `ICB Code`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$gender <- gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$ageband <- ageband_data_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$age_gender <- age_gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$imd <- imd_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$monthly_section <- national_extract_monthly %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Identified Patient Flag`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$monthly_paragraph <- paragraph_extract_monthly %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$monthly_chem_substance <-
  chem_sub_extract_monthly %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$avg_per_pat <- chem_sub_extract_monthly |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0402",
                `Identified Patient Flag` == "Y") |>
  dplyr::mutate(
    `Average Items per patient` = (`Total Items` / `Total Identified Patients`),
    `Average NIC per Patient (GBP)` = (`Total Net Ingredient Cost (GBP)` /
                                         `Total Identified Patients`)
  )

quarterly_0402$pat_per_1000_pop <- age_gender_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::ungroup() |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`
  ) |>
  dplyr::filter(`BNF Section Code` == "0402") |>
  dplyr::mutate(`Mid-year Population Year` = as.numeric((substr(
    c(`Financial Year`), 1, 4
  ))), .after = `Financial Year`) |>
  dplyr::filter(`Identified Patient Flag` == "Y",
                `Age Band` != "Unknown") |>
  stats::na.omit() |>
  dplyr::left_join(
    select(
      pop_agegen_2022,
      `Year`,
      `Sex`,
      `Age Band`,
      `Mid-year Population Estimate`
    ),
    by = c(
      "Mid-year Population Year" = "Year",
      "Patient Gender" = "Sex",
      "Age Band" = "Age Band"
    )
  ) |>
  dplyr::mutate(`Patients per 1,000 Population` = ((`Total Identified Patients` /
                                                      `Mid-year Population Estimate`) * 1000
  ))

# 0403 Antidepressants workbook - quarterly

quarterly_0403 <- list()

quarterly_0403$patient_id <- capture_rate_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$national_total <- national_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$national_paragraph <- paragraph_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$icb <-  icb_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `ICB Name`,
    `ICB Code`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$gender <- gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$ageband <- ageband_data_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$age_gender <- age_gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$imd <- imd_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$prescribing_in_children <-
  child_adult_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$monthly_section <- national_extract_monthly %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Identified Patient Flag`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$monthly_paragraph <- paragraph_extract_monthly %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0403")

# add chemical substance level in monthly tables if needed
quarterly_0403$monthly_chem_substance <-
  chem_sub_extract_monthly %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$avg_per_pat <- chem_sub_extract_monthly |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0403",
                `Identified Patient Flag` == "Y") |>
  dplyr::mutate(
    `Average Items per patient` = (`Total Items` / `Total Identified Patients`),
    `Average NIC per Patient (GBP)` = (`Total Net Ingredient Cost (GBP)` /
                                         `Total Identified Patients`)
  )

quarterly_0403$pat_per_1000_pop <- age_gender_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::ungroup() |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`
  ) |>
  dplyr::filter(`BNF Section Code` == "0403") |>
  dplyr::mutate(`Mid-year Population Year` = as.numeric((substr(
    c(`Financial Year`), 1, 4
  ))), .after = `Financial Year`) |>
  dplyr::filter(`Identified Patient Flag` == "Y",
                `Age Band` != "Unknown") |>
  stats::na.omit() |>
  dplyr::left_join(
    select(
      pop_agegen_2022,
      `Year`,
      `Sex`,
      `Age Band`,
      `Mid-year Population Estimate`
    ),
    by = c(
      "Mid-year Population Year" = "Year",
      "Patient Gender" = "Sex",
      "Age Band" = "Age Band"
    )
  ) |>
  dplyr::mutate(`Patients per 1,000 Population` = ((`Total Identified Patients` /
                                                      `Mid-year Population Estimate`) * 1000
  )) 

# 0404 CNS stimulants and drugs used for ADHD - quarterly

quarterly_0404 <- list()

quarterly_0404$patient_id <- capture_rate_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$national_total <- national_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$national_chem_substance <-
  chem_sub_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$icb <-  icb_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `ICB Name`,
    `ICB Code`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$gender <- gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$ageband <- ageband_data_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$age_gender <- age_gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$imd <- imd_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$prescribing_in_children <-
  child_adult_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$monthly_section <- national_extract_monthly %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Identified Patient Flag`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$monthly_chem_substance <-
  chem_sub_extract_monthly %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$avg_per_pat <- chem_sub_extract_monthly |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0404",
                `Identified Patient Flag` == "Y") |>
  dplyr::mutate(
    `Average Items per patient` = (`Total Items` / `Total Identified Patients`),
    `Average NIC per Patient (GBP)` = (`Total Net Ingredient Cost (GBP)` /
                                         `Total Identified Patients`)
  )

quarterly_0404$pat_per_1000_pop <- age_gender_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::ungroup() |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`
  ) |>
  dplyr::filter(`BNF Section Code` == "0404") |>
  dplyr::mutate(`Mid-year Population Year` = as.numeric((substr(
    c(`Financial Year`), 1, 4
  ))), .after = `Financial Year`) |>
  dplyr::filter(`Identified Patient Flag` == "Y",
                `Age Band` != "Unknown") |>
  stats::na.omit() |>
  dplyr::left_join(
    select(
      pop_agegen_2022,
      `Year`,
      `Sex`,
      `Age Band`,
      `Mid-year Population Estimate`
    ),
    by = c(
      "Mid-year Population Year" = "Year",
      "Patient Gender" = "Sex",
      "Age Band" = "Age Band"
    )
  ) |>
  dplyr::mutate(`Patients per 1,000 Population` = ((`Total Identified Patients` /
                                                      `Mid-year Population Estimate`) * 1000
  ))

# 0411 Drugs for dementia workbook - quarterly

quarterly_0411 <- list()

quarterly_0411$patient_id <- capture_rate_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$national_total <- national_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$national_chem_substance <-
  chem_sub_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$icb <-  icb_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `ICB Name`,
    `ICB Code`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$gender <- gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$ageband <- ageband_data_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$age_gender <- age_gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$imd <- imd_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$monthly_section <- national_extract_monthly %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Identified Patient Flag`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$monthly_chem_substance <-
  chem_sub_extract_monthly %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$avg_per_pat <- chem_sub_extract_monthly |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `Year Month`,
    `BNF Section Name`,
    `BNF Section Code`,
    `BNF Paragraph Name`,
    `BNF Paragraph Code`,
    `BNF Chemical Substance Name`,
    `BNF Chemical Substance Code`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)` = `sdc_Total Net Ingredient Cost (GBP)`
  ) |>
  dplyr::filter(`BNF Section Code` == "0411",
                `Identified Patient Flag` == "Y") |>
  dplyr::mutate(
    `Average Items per patient` = (`Total Items` / `Total Identified Patients`),
    `Average NIC per Patient (GBP)` = (`Total Net Ingredient Cost (GBP)` /
                                         `Total Identified Patients`)
  )

quarterly_0411$pat_per_1000_pop <- age_gender_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::ungroup() |>
  dplyr::select(
    `Financial Year`,
    `Financial Quarter`,
    `BNF Section Name`,
    `BNF Section Code`,
    `Age Band`,
    `Patient Gender`,
    `Identified Patient Flag`,
    `Total Identified Patients` = `sdc_Total Identified Patients`
  ) |>
  dplyr::filter(`BNF Section Code` == "0411") |>
  dplyr::mutate(`Mid-year Population Year` = as.numeric((substr(
    c(`Financial Year`), 1, 4
  ))), .after = `Financial Year`) |>
  dplyr::filter(`Identified Patient Flag` == "Y",
                `Age Band` != "Unknown") |>
  stats::na.omit() |>
  dplyr::left_join(
    select(
      pop_agegen_2022,
      `Year`,
      `Sex`,
      `Age Band`,
      `Mid-year Population Estimate`
    ),
    by = c(
      "Mid-year Population Year" = "Year",
      "Patient Gender" = "Sex",
      "Age Band" = "Age Band"
    )
  ) |>
  dplyr::mutate(`Patients per 1,000 Population` = ((`Total Identified Patients` /
                                                      `Mid-year Population Estimate`) * 1000
  ))

# 4. Model of pre-COVID-19-pandemic prescribing trends -------------------------

# model of item predictions extrapolated from pre-covid pandemic prescribing trends

# get dispensing days for up to 2023/24 financial year
dispensing_days_data <- nhsbsaUtils::dispensing_days(2024)

# aggregate to 20 year agebands and calculate month position variables
df20 <- ageband_manip_20yr(age_gender_extract_month)

# split df20 into pre-March-2020 months, train model on pre-pandemic data only
df20_pc <- df20 |>
  dplyr::filter(time_period == "pre_covid")

# build model for each BNF section, using pre-pandemic items
model_0401 <- covid_lm(df20_pc,
                       section_code = "0401")
model_0402 <- covid_lm(df20_pc,
                       section_code = "0402")
model_0403 <- covid_lm(df20_pc,
                       section_code = "0403")
model_0404 <- covid_lm(df20_pc,
                       section_code = "0404")
model_0411 <- covid_lm(df20_pc,
                       section_code = "0411")

# predict number of items by BNF section per month
# predictions are extrapolations of pre-COVID-19 trends, applied to full df20 data
# so will only predict for March 2020 onwards

predictions_0401 <- prediction_list(df20,
                                    "0401",
                                    model_0401,
                                    pred_month_list)
predictions_0402 <- prediction_list(df20,
                                    "0402",
                                    model_0402,
                                    pred_month_list)
predictions_0403 <- prediction_list(df20,
                                    "0403",
                                    model_0403,
                                    pred_month_list)
predictions_0404 <- prediction_list(df20,
                                    "0404",
                                    model_0404,
                                    pred_month_list)
predictions_0411 <- prediction_list(df20,
                                    "0411",
                                    model_0411,
                                    pred_month_list)

#save covid estimated items to excel wb for QR
covid_model_predictions_sep <- rbind(
  predictions_0401,
  predictions_0402,
  predictions_0403,
  predictions_0404,
  predictions_0411
)

# update month in file name for new publications
fwrite(covid_model_predictions_sep, "Y:/Official Stats/MUMH/Covid model tables/Sep23.csv")

# 5. Data tables ---------------------------------------------------------------

# data tables for spreadsheet outputs
# formatted according to accessibility standards
# user may need to update file name to write outputs to in future releases

# 0401 Hypnotics and anxiolytics workbook - quarterly

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "National_Paragraph",
  "ICB",
  "Gender",
  "Age_Band",
  "Age_Band_and_Gender",
  "Population_by_Age_Gender",
  "IMD",
  "Monthly_Section",
  "Monthly_Paragraph",
  "Monthly_Chemical_Substance",
  "Average_Items_per_Patient"
)

wb <- accessibleTables::create_wb(sheetNames)

# create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "BNF Section Name",
  "BNF Section Code",
  "Identified Patient Flag",
  "Total Identified Patients",
  "Total Items",
  "Total Net Ingredient Cost (GBP)",
  "Financial Year",
  "Financial Quarter",
  "Year Month",
  "BNF Paragraph Name",
  "BNF Paragraph Code",
  "ICB Name",
  "ICB Code",
  "BNF Chemical Substance Name",
  "BNF Chemical Substance Code",
  "Patient Gender",
  "Age Band",
  "IMD Quintile",
  "Average Items per Patient",
  "Average NIC per patient (GBP)",
  "Mid-Year England Population Estimate",
  "Mid-Year Population Year",
  "Patients per 1,000 Population"
)

meta_descs <-
  c(
    "The name given to a British National Formulary (BNF) section. This is the next broadest grouping of The BNF Therapeutical classification system after chapter.",
    "The unique code used to refer to the British National Formulary (BNF) section.",
    "This shows where an item has been attributed to an NHS number that has been verified by the Personal Demographics Service.",
    "Where patients are identified via the flag, the number of patients that the data corresponds to. This will always be 0 where 'Identified Patient' = N.",
    "The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
    "Total Net Ingredient Cost is the amount that would be paid using the basic price of the prescribed drug or appliance and the quantity prescribed. Sometimes called the 'Net Ingredient Cost' (NIC). The basic price is given either in the Drug Tariff or is determined from prices published by manufacturers, wholesalers or suppliers. Basic price is set out in Parts 8 and 9 of the Drug Tariff. For any drugs or appliances not in Part 8, the price is usually taken from the manufacturer, wholesaler or supplier of the product. This is given in GBP (£).",
    "The financial year to which the data belongs.",
    "The financial quarter to which the data belongs.",
    "The year and month to which the data belongs, denoted in YYYYMM format.",
    "The name given to the British National Formular (BNF) paragraph. This level of grouping of the BNF Therapeutical classification system sits below BNF section.",
    "The unique code used to refer to the British National Formulary (BNF) paragraph.",
    "The name given to the Integrated Care Board (ICB) that a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
    "The unique code used to refer to an Integrated Care Board (ICB).",
    "The name of the main active ingredient in a drug. Appliances do not hold a chemical substance, but instead inherit the corresponding BNF section. Determined by the British National Formulatory (BNF) for drugs, or the NHS BSA for appliances. For example, Amoxicillin.",
    "The unique code used to refer to the British National Formulary (BNF) chemical substance.",
    "The gender of the patient as at the time the prescription was processed. Please see the detailed Background Information and Methodology notice released with this publication for further information.",
    "The age band of the patient as of the 30th September of the corresponding financial year the drug was prescribed.",
    "The IMD quintile of the patient, based on the patient's postcode, where '1' is the 20% of areas with the highest deprivation score in the Index of Multiple Deprivation (IMD) from the English Indices of Deprivation 2019, and '5' is the 20% of areas with the lowest IMD deprivation score. The IMD quintile has been recorded as 'Unknown' where the items are attributed to an unidentified patient, or where we have been unable to match the patient postcode to a postcode in the National Statistics Postcode Lookup (NSPL).",
    "The total number of items divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean.",
    "The total Net Ingredient Cost divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean, and is given in GBP (£).",
    "The population estimate for the corresponding Mid-Year Population Year.",
    "The year in which population estimates were taken, required due to the presentation of this data in financial year format.",
    "The number of identified patients by age band and gender divided by mid-year population of the same age band and gender group in England, multiplied by 1,000. Only identified patients with a known gender and age band are included. This is calculated by (Total Identified Patients / Mid-Year England Population Estimate) * 1000."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)

# patient identification

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Patient_Identification",
  paste0(
    config$publication_table_title,
    " - Proportion of items for which an NHS number was recorded (%)"
  ),
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0401$patient_id,
  42
)

# left align columns A to B
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

# right align columns, round to 2 decimal places
accessibleTables::format_data(
  wb,
  "Patient_Identification",
  c(
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "AA",
    "AB",
    "AC",
    "AD",
    "AE",
    "AF",
    "AG",
    "AH",
    "AI",
    "AJ"
  ),
  "right",
  "0.00"
)

# national total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$national_total,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


# national paragraph

accessibleTables::write_sheet(
  wb,
  "National_Paragraph",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by BNF paragraph and identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$national_paragraph,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("H", "I"),
                              "right",
                              "#,##0")

# right align column J and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("J"),
                              "right",
                              "#,##0.00")

# ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  paste0(config$publication_table_title, " - Quarterly totals by ICB"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$icb,
  14
)

# left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

# right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

# right align column N and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

# gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  paste0(config$publication_table_title, " - Quarterly totals by gender"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$gender,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

# age band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  paste0(config$publication_table_title, " - Quarterly totals by age band"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$ageband,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

# age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly totals by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$age_gender,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

# right align column J and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

# patients per 1000 population by age and gender

accessibleTables::write_sheet(
  wb,
  "Population_by_Age_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly population totals split by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "5. ONS population estimates for 2023/2024 were not available prior to publication. ONS population estimates taken from https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales",
    "6. Patients per 1,000 population is an age-gender specific rate. These rates should only be analysed at the level at which they are presented and should not be used to compare across BNF sections."
  ),
  quarterly_0401$pat_per_1000_pop,
  14
)

# left align columns A to H
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("A", "B", "C", "D", "E", "F", "G", "H"),
                              "left",
                              "")

# right align columns I and J and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("I", "J"),
                              "right",
                              "#,##0")

# right align column K and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("K"),
                              "right",
                              "#,##0.00")

# IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  paste0(config$publication_table_title, " - Quarterly totals by IMD"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$imd,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

# monthly section data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF section"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$monthly_section,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("I"),
                              "right",
                              "#,##0.00")

# monthly paragraph data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Paragraph",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF paragraph"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$monthly_paragraph,
  14
)

# left align columns A to H
accessibleTables::format_data(wb,
                              "Monthly_Paragraph",
                              c("A", "B", "C", "D", "E", "F", "G", "H"),
                              "left",
                              "")

# right align columns I and J and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Paragraph",
                              c("I", "J"),
                              "right",
                              "#,##0")

# right align column K and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Paragraph",
                              c("K"),
                              "right",
                              "#,##0.00")

# monthly chemical substance data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Chemical_Substance",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0401$monthly_chem_substance,
  14
)

# left align columns A to J
accessibleTables::format_data(
  wb,
  "Monthly_Chemical_Substance",
  c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
  "left",
  ""
)

# right align columns K and L and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("K", "L"),
                              "right",
                              "#,##0")

# right align column M and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("M"),
                              "right",
                              "#,##0.00")

# average items per patient monthly chemical substance data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Average_Items_per_Patient",
  paste0(
    config$publication_table_title,
    " - Average items per patient by month and BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "4. These averages refer to the mean. Average items per patient and average Net Ingredient Cost (NIC) per patient have only been calculated using items and costs associated with the identified patient flag. More information can be found in the Metadata sheet."
  ),
  quarterly_0401$avg_per_pat,
  14
)

# left align columns A to J
accessibleTables::format_data(
  wb,
  "Average_Items_per_Patient",
  c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
  "left",
  ""
)

# right align columns K and L and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("K", "L"),
                              "right",
                              "#,##0")

# right align column M, N, and O and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("M", "N", "O"),
                              "right",
                              "#,##0.00")


# create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0401 Hypnotics and anxiolytics",
  config$cover_sheet_sub_title,
  paste0("Publication date: ", config$publication_date),
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: Patient Identification Rates",
    "Table 2: National Total",
    "Table 3: National Paragraph",
    "Table 4: ICB",
    "Table 5: Gender",
    "Table 6: Age Band",
    "Table 7: Age Band and Gender",
    "Table 8: Population by Age Band and Gender",
    "Table 9: Indices of Deprivation (IMD)",
    "Table 10: Monthly Section",
    "Table 11: Monthly Paragraph",
    "Table 12: Monthly Chemical Substance",
    "Table 13: Average Items per Identified Patient"
  ),
  c("Metadata", sheetNames)
)

# save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0401_Sep_23.xlsx",
                       overwrite = TRUE)

# 0402 Drugs used in psychoses and related disorders - quarterly and monthly

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "National_Paragraph",
  "ICB",
  "Gender",
  "Age_Band",
  "Age_Band_and_Gender",
  "Population_by_Age_Gender",
  "IMD",
  "Monthly_Section",
  "Monthly_Paragraph",
  "Monthly_Chemical_Substance",
  "Average_Items_per_Patient"
)

wb <- accessibleTables::create_wb(sheetNames)

# create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "BNF Section Name",
  "BNF Section Code",
  "Identified Patient Flag",
  "Total Identified Patients",
  "Total Items",
  "Total Net Ingredient Cost (GBP)",
  "Financial Year",
  "Financial Quarter",
  "Year Month",
  "BNF Paragraph Name",
  "BNF Paragraph Code",
  "ICB Name",
  "ICB Code",
  "BNF Chemical Substance Name",
  "BNF Chemical Substance Code",
  "Patient Gender",
  "Age Band",
  "IMD Quintile",
  "Average items per patient",
  "Average NIC per patient (GBP)",
  "Mid-Year England Population Estimate",
  "Mid-Year Population Year",
  "Patients per 1,000 Population"
)

meta_descs <-
  c(
    "The name given to a British National Formulary (BNF) section. This is the next broadest grouping of The BNF Therapeutical classification system after chapter.",
    "The unique code used to refer to the British National Formulary (BNF) section.",
    "This shows where an item has been attributed to an NHS number that has been verified by the Personal Demographics Service.",
    "Where patients are identified via the flag, the number of patients that the data corresponds to. This will always be 0 where 'Identified Patient' = N.",
    "The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
    "Total Net Ingredient Cost is the amount that would be paid using the basic price of the prescribed drug or appliance and the quantity prescribed. Sometimes called the 'Net Ingredient Cost' (NIC). The basic price is given either in the Drug Tariff or is determined from prices published by manufacturers, wholesalers or suppliers. Basic price is set out in Parts 8 and 9 of the Drug Tariff. For any drugs or appliances not in Part 8, the price is usually taken from the manufacturer, wholesaler or supplier of the product. This is given in GBP (£).",
    "The financial year to which the data belongs.",
    "The financial quarter to which the data belongs.",
    "The year and month to which the data belongs, denoted in YYYYMM format.",
    "The name given to the British National Formular (BNF) paragraph. This level of grouping of the BNF Therapeutical classification system sits below BNF section.",
    "The unique code used to refer to the British National Formulary (BNF) paragraph.",
    "The name given to the Integrated Care Board (ICB) that a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
    "The unique code used to refer to an Integrated Care Board (ICB).",
    "The name of the main active ingredient in a drug. Appliances do not hold a chemical substance, but instead inherit the corresponding BNF section. Determined by the British National Formulatory (BNF) for drugs, or the NHS BSA for appliances. For example, Amoxicillin.",
    "The unique code used to refer to the British National Formulary (BNF) chemical substance.",
    "The gender of the patient as at the time the prescription was processed. Please see the detailed Background Information and Methodology notice released with this publication for further information.",
    "The age band of the patient as of the 30th September of the corresponding financial year the drug was prescribed.",
    "The IMD quintile of the patient, based on the patient's postcode, where '1' is the 20% of areas with the highest deprivation score in the Index of Multiple Deprivation (IMD) from the English Indices of Deprivation 2019, and '5' is the 20% of areas with the lowest IMD deprivation score. The IMD quintile has been recorded as 'Unknown' where the items are attributed to an unidentified patient, or where we have been unable to match the patient postcode to a postcode in the National Statistics Postcode Lookup (NSPL).",
    "The total number of items divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean.",
    "The total Net Ingredient Cost divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean, and is given in GBP (£).",
    "The population estimate for the corresponding Mid-Year Population Year.",
    "The year in which population estimates were taken, required due to the presentation of this data in financial year format.",
    "The number of identified patients by age band and gender divided by mid-year population of the same age band and gender group in England, multiplied by 1,000. Only identified patients with a known gender and age band are included. This is calculated by (Total Identified Patients / Mid-Year England Population Estimate) * 1000."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)

# patient identification

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Patient_Identification",
  paste0(
    config$publication_table_title,
    " - Proportion of items for which an NHS number was recorded (%)"
  ),
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0402$patient_id,
  42
)

# left align columns A to B
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

#right align columns and round to 2 decimal places 
accessibleTables::format_data(
  wb,
  "Patient_Identification",
  c(
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "AA",
    "AB",
    "AC",
    "AD",
    "AE",
    "AF",
    "AG",
    "AH",
    "AI",
    "AJ"
  ),
  "right",
  "0.00"
)

# national total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$national_total,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


# national paragraph

accessibleTables::write_sheet(
  wb,
  "National_Paragraph",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by BNF paragraph and identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$national_paragraph,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column J and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("J"),
                              "right",
                              "#,##0.00")

# ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  paste0(config$publication_table_title, " - Quarterly totals by ICB"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$icb,
  14
)

# left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

# right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

# right align column N and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

# gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  paste0(config$publication_table_title, " - Quarterly totals by gender"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$gender,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

# age band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  paste0(config$publication_table_title, " - Quarterly totals by age band"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$ageband,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

# age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly totals by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$age_gender,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

# right align column J and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

# patients per 1000 population by age and gender

accessibleTables::write_sheet(
  wb,
  "Population_by_Age_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly population totals split by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "5. ONS population estimates for 2023/2024 were not available prior to publication. ONS population estimates taken from https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales",
    "6. Patients per 1,000 population is an age-gender specific rate. These rates should only be analysed at the level at which they are presented and should not be used to compare across BNF sections."
  ),
  quarterly_0402$pat_per_1000_pop,
  14
)

# left align columns A to H
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("A", "B", "C", "D", "E", "F", "G", "H"),
                              "left",
                              "")

# right align columns I and J and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("I", "J"),
                              "right",
                              "#,##0")

# right align column K and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("K"),
                              "right",
                              "#,##0.00")

# IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  paste0(config$publication_table_title, " - Quarterly totals by IMD"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$imd,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

# monthly section data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF section"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$monthly_section,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("I"),
                              "right",
                              "#,##0.00")

# monthly paragraph data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Paragraph",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF paragraph"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$monthly_paragraph,
  14
)

# left align columns A to H
accessibleTables::format_data(wb,
                              "Monthly_Paragraph",
                              c("A", "B", "C", "D", "E", "F", "G", "H"),
                              "left",
                              "")

# right align columns I and J and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Paragraph",
                              c("I", "J"),
                              "right",
                              "#,##0")

# right align column K and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Paragraph",
                              c("K"),
                              "right",
                              "#,##0.00")

# monthly chemical substance data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Chemical_Substance",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0402$monthly_chem_substance,
  14
)

# left align columns A to J
accessibleTables::format_data(
  wb,
  "Monthly_Chemical_Substance",
  c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
  "left",
  ""
)

# right align columns K and L and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("K", "L"),
                              "right",
                              "#,##0")

# right align column M and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("M"),
                              "right",
                              "#,##0.00")

# average items per patient monthly chemical substance data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Average_Items_per_Patient",
  paste0(
    config$publication_table_title,
    " - Average items per patient by month and BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "4. These averages refer to the mean. Average items per patient and average Net Ingredient Cost (NIC) per patient have only been calculated using items and costs associated with the identified patient flag. More information can be found in the Metadata sheet."
  ),
  quarterly_0402$avg_per_pat,
  14
)

# left align columns A to J
accessibleTables::format_data(
  wb,
  "Average_Items_per_Patient",
  c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
  "left",
  ""
)

# right align columns K and L and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("K", "L"),
                              "right",
                              "#,##0")

# right align column M, N, and O and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("M", "N", "O"),
                              "right",
                              "#,##0.00")

# create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0402 Drugs used in psychoses and related disorders",
  config$cover_sheet_sub_title,
  paste0("Publication date: ", config$publication_date),
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: Patient Identification Rates",
    "Table 2: National Total",
    "Table 3: National Paragraph",
    "Table 4: ICB",
    "Table 5: Gender",
    "Table 6: Age Band",
    "Table 7: Age Band and Gender",
    "Table 8: Population by Age Band and Gender",
    "Table 9: Indices of Deprivation (IMD)",
    "Table 10: Monthly Section",
    "Table 11: Monthly Paragraph",
    "Table 12: Monthly Chemical Substance",
    "Table 13: Average Items per Identified Patient"
  ),
  c("Metadata", sheetNames)
)

# save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0402_Sep_23.xlsx",
                       overwrite = TRUE)


# 0403 Antidepressants - quarterly and monthly

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "National_Paragraph",
  "ICB",
  "Gender",
  "Age_Band",
  "Age_Band_and_Gender",
  "Population_by_Age_Gender",
  "IMD",
  "Presc_in_Children",
  "Monthly_Section",
  "Monthly_Paragraph",
  "Monthly_Chemical_Substance",
  "Average_Items_per_Patient"
)

wb <- accessibleTables::create_wb(sheetNames)

# create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "BNF Section Name",
  "BNF Section Code",
  "Identified Patient Flag",
  "Total Identified Patients",
  "Total Items",
  "Total Net Ingredient Cost (GBP)",
  "Financial Year",
  "Financial Quarter",
  "Year Month",
  "BNF Paragraph Name",
  "BNF Paragraph Code",
  "ICB Name",
  "ICB Code",
  "BNF Chemical Substance Name",
  "BNF Chemical Substance Code",
  "Patient Gender",
  "Age Band",
  "IMD Quintile",
  "Average Items per Patient",
  "Average NIC per Patient (GBP)",
  "Mid-Year England Population Estimate",
  "Mid-Year Population Year",
  "Patients per 1,000 Population"
)

meta_descs <-
  c(
    "The name given to a British National Formulary (BNF) section. This is the next broadest grouping of The BNF Therapeutical classification system after chapter.",
    "The unique code used to refer to the British National Formulary (BNF) section.",
    "This shows where an item has been attributed to an NHS number that has been verified by the Personal Demographics Service.",
    "Where patients are identified via the flag, the number of patients that the data corresponds to. This will always be 0 where 'Identified Patient' = N.",
    "The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
    "Total Net Ingredient Cost is the amount that would be paid using the basic price of the prescribed drug or appliance and the quantity prescribed. Sometimes called the 'Net Ingredient Cost' (NIC). The basic price is given either in the Drug Tariff or is determined from prices published by manufacturers, wholesalers or suppliers. Basic price is set out in Parts 8 and 9 of the Drug Tariff. For any drugs or appliances not in Part 8, the price is usually taken from the manufacturer, wholesaler or supplier of the product. This is given in GBP (£).",
    "The financial year to which the data belongs.",
    "The financial quarter to which the data belongs.",
    "The year and month to which the data belongs, denoted in YYYYMM format.",
    "The name given to the British National Formular (BNF) paragraph. This level of grouping of the BNF Therapeutical classification system sits below BNF section.",
    "The unique code used to refer to the British National Formulary (BNF) paragraph.",
    "The name given to the Integrated Care Board (ICB) that a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
    "The unique code used to refer to an Integrated Care Board (ICB).",
    "The name of the main active ingredient in a drug. Appliances do not hold a chemical substance, but instead inherit the corresponding BNF section. Determined by the British National Formulatory (BNF) for drugs, or the NHS BSA for appliances. For example, Amoxicillin.",
    "The unique code used to refer to the British National Formulary (BNF) chemical substance.",
    "The gender of the patient as at the time the prescription was processed. Please see the detailed Background Information and Methodology notice released with this publication for further information.",
    "The age band of the patient as of the 30th September of the corresponding financial year the drug was prescribed.",
    "The IMD quintile of the patient, based on the patient's postcode, where '1' is the 20% of areas with the highest deprivation score in the Index of Multiple Deprivation (IMD) from the English Indices of Deprivation 2019, and '5' is the 20% of areas with the lowest IMD deprivation score. The IMD quintile has been recorded as 'Unknown' where the items are attributed to an unidentified patient, or where we have been unable to match the patient postcode to a postcode in the National Statistics Postcode Lookup (NSPL).",
    "The total number of items divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean.",
    "The total Net Ingredient Cost divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean, and is given in GBP (£).",
    "The population estimate for the corresponding Mid-Year Population Year.",
    "The year in which population estimates were taken, required due to the presentation of this data in financial year format.",
    "The number of identified patients by age band and gender divided by mid-year population of the same age band and gender group in England, multiplied by 1,000. Only identified patients with a known gender and age band are included. This is calculated by (Total Identified Patients / Mid-Year England Population Estimate) * 1000."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)

# patient identification

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Patient_Identification",
  paste0(
    config$publication_table_title,
    " - Proportion of items for which an NHS number was recorded (%)"
  ),
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0403$patient_id,
  42
)

# left align columns A to B
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

#right align columns and round to 2 decimal places
accessibleTables::format_data(
  wb,
  "Patient_Identification",
  c(
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "AA",
    "AB",
    "AC",
    "AD",
    "AE",
    "AF",
    "AG",
    "AH",
    "AI",
    "AJ"
  ),
  "right",
  "0.00"
)

# national total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$national_total,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


# national paragraph

accessibleTables::write_sheet(
  wb,
  "National_Paragraph",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by BNF paragraph and identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$national_paragraph,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("H", "I"),
                              "right",
                              "#,##0")

# right align column J and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("J"),
                              "right",
                              "#,##0.00")

# ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  paste0(config$publication_table_title, " - Quarterly totals by ICB"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$icb,
  14
)

# left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

# right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

# right align column N and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

# gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  paste0(config$publication_table_title, " - Quarterly totals by gender"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$gender,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

# age band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  paste0(config$publication_table_title, " - Quarterly totals by age band"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$ageband,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

# age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly totals by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$age_gender,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

# right align column J and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

# patients per 1000 population by age and gender

accessibleTables::write_sheet(
  wb,
  "Population_by_Age_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly population totals split by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "5. ONS population estimates for 2023/2024 were not available prior to publication. ONS population estimates taken from https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales",
    "6. Patients per 1,000 population is an age-gender specific rate. These rates should only be analysed at the level at which they are presented and should not be used to compare across BNF sections."
  ),
  quarterly_0403$pat_per_1000_pop,
  14
)

# left align columns A to H
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("A", "B", "C", "D", "E", "F", "G", "H"),
                              "left",
                              "")

# right align columns I and J and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("I", "J"),
                              "right",
                              "#,##0")

# right align column K and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("K"),
                              "right",
                              "#,##0.00")

# IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  paste0(config$publication_table_title, " - Quarterly totals by IMD"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$imd,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

# prescribing in children

accessibleTables::write_sheet(
  wb,
  "Presc_in_Children",
  paste0(
    config$publication_table_title,
    " - Prescribing in adults and children, age bands 17 and under to 18 and over"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$prescribing_in_children,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "Presc_in_Children",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Presc_in_Children",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Presc_in_Children",
                              c("H"),
                              "right",
                              "#,##0.00")

# monthly section data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF section"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$monthly_section,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("I"),
                              "right",
                              "#,##0.00")

# monthly paragraph data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Paragraph",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF paragraph"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$monthly_paragraph,
  14
)

# left align columns A to H
accessibleTables::format_data(wb,
                              "Monthly_Paragraph",
                              c("A", "B", "C", "D", "E", "F", "G", "H"),
                              "left",
                              "")

# right align columns I and J and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Paragraph",
                              c("I", "J"),
                              "right",
                              "#,##0")

# right align column K and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Paragraph",
                              c("K"),
                              "right",
                              "#,##0.00")

# monthly chemical substance data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Chemical_Substance",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0403$monthly_chem_substance,
  14
)

# left align columns A to J
accessibleTables::format_data(
  wb,
  "Monthly_Chemical_Substance",
  c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
  "left",
  ""
)

# right align columns K and L and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("K", "L"),
                              "right",
                              "#,##0")

# right align column M and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("M"),
                              "right",
                              "#,##0.00")

# average items per patient monthly chemical substance data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Average_Items_per_Patient",
  paste0(
    config$publication_table_title,
    " - Average items per patient by month and BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "4. These averages refer to the mean. Average items per patient and average Net Ingredient Cost (NIC) per patient have only been calculated using items and costs associated with the identified patient flag. More information can be found in the Metadata sheet."
  ),
  quarterly_0403$avg_per_pat,
  14
)

# left align columns A to J
accessibleTables::format_data(
  wb,
  "Average_Items_per_Patient",
  c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
  "left",
  ""
)

# right align columns K and L and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("K", "L"),
                              "right",
                              "#,##0")

# right align column M, N, and O and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("M", "N", "O"),
                              "right",
                              "#,##0.00")


# create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0403 Antidepressant drugs",
  config$cover_sheet_sub_title,
  paste0("Publication date: ", config$publication_date),
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: Patient Identification Rates",
    "Table 2: National Total",
    "Table 3: National Paragraph",
    "Table 4: ICB",
    "Table 5: Gender",
    "Table 6: Age Band",
    "Table 7: Age Band and Gender",
    "Table 8: Population by Age Band and Gender",
    "Table 9: Indices of Deprivation (IMD)",
    "Table 10: Prescribing in Children",
    "Table 11: Monthly Section",
    "Table 12: Monthly Paragraph",
    "Table 13: Monthly Chemical Substance",
    "Table 14: Average Items per Identified Patient"
  ),
  c("Metadata", sheetNames)
)

# save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0403_Sep_23.xlsx",
                       overwrite = TRUE)

# 0404 CNS stimulants and drugs used for ADHD - quarterly and monthly

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "National_Chemical_Substance",
  "ICB",
  "Gender",
  "Age_Band",
  "Age_Band_and_Gender",
  "Population_by_Age_Gender",
  "IMD",
  "Presc_in_Children",
  "Monthly_Section",
  "Monthly_Chemical_Substance",
  "Average_Items_per_Patient"
)

wb <- accessibleTables::create_wb(sheetNames)

# create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "BNF Section Name",
  "BNF Section Code",
  "Identified Patient Flag",
  "Total Identified Patients",
  "Total Items",
  "Total Net Ingredient Cost (GBP)",
  "Financial Year",
  "Financial Quarter",
  "Year Month",
  "BNF Paragraph Name",
  "BNF Paragraph Code",
  "ICB Name",
  "ICB Code",
  "BNF Chemical Substance Name",
  "BNF Chemical Substance Code",
  "Patient Gender",
  "Age Band",
  "IMD Quintile",
  "Average Items per Patient",
  "Average NIC per Patient (GBP)",
  "Mid-Year England Population Estimate",
  "Mid-Year Population Year",
  "Patients per 1,000 Population"
)

meta_descs <-
  c(
    "The name given to a British National Formulary (BNF) section. This is the next broadest grouping of The BNF Therapeutical classification system after chapter.",
    "The unique code used to refer to the British National Formulary (BNF) section.",
    "This shows where an item has been attributed to an NHS number that has been verified by the Personal Demographics Service.",
    "Where patients are identified via the flag, the number of patients that the data corresponds to. This will always be 0 where 'Identified Patient' = N.",
    "The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
    "Total Net Ingredient Cost is the amount that would be paid using the basic price of the prescribed drug or appliance and the quantity prescribed. Sometimes called the 'Net Ingredient Cost' (NIC). The basic price is given either in the Drug Tariff or is determined from prices published by manufacturers, wholesalers or suppliers. Basic price is set out in Parts 8 and 9 of the Drug Tariff. For any drugs or appliances not in Part 8, the price is usually taken from the manufacturer, wholesaler or supplier of the product. This is given in GBP (£).",
    "The financial year to which the data belongs.",
    "The financial quarter to which the data belongs.",
    "The year and month to which the data belongs, denoted in YYYYMM format.",
    "The name given to the British National Formular (BNF) paragraph. This level of grouping of the BNF Therapeutical classification system sits below BNF section.",
    "The unique code used to refer to the British National Formulary (BNF) paragraph.",
    "The name given to the Integrated Care Board (ICB) that a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
    "The unique code used to refer to an Integrated Care Board (ICB).",
    "The name of the main active ingredient in a drug. Appliances do not hold a chemical substance, but instead inherit the corresponding BNF section. Determined by the British National Formulatory (BNF) for drugs, or the NHS BSA for appliances. For example, Amoxicillin.",
    "The unique code used to refer to the British National Formulary (BNF) chemical substance.",
    "The gender of the patient as at the time the prescription was processed. Please see the detailed Background Information and Methodology notice released with this publication for further information.",
    "The age band of the patient as of the 30th September of the corresponding financial year the drug was prescribed.",
    "The IMD quintile of the patient, based on the patient's postcode, where '1' is the 20% of areas with the highest deprivation score in the Index of Multiple Deprivation (IMD) from the English Indices of Deprivation 2019, and '5' is the 20% of areas with the lowest IMD deprivation score. The IMD quintile has been recorded as 'Unknown' where the items are attributed to an unidentified patient, or where we have been unable to match the patient postcode to a postcode in the National Statistics Postcode Lookup (NSPL).",
    "The total number of items divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean.",
    "The total Net Ingredient Cost divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean, and is given in GBP (£).",
    "The population estimate for the corresponding Mid-Year Population Year.",
    "The year in which population estimates were taken, required due to the presentation of this data in financial year format.",
    "The number of identified patients by age band and gender divided by mid-year population of the same age band and gender group in England, multiplied by 1,000. Only identified patients with a known gender and age band are included. This is calculated by (Total Identified Patients / Mid-Year England Population Estimate) * 1000."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)

# patient identification

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Patient_Identification",
  paste0(
    config$publication_table_title,
    " - Proportion of items for which an NHS number was recorded (%)"
  ),
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0404$patient_id,
  42
)

# left align columns A to B
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

# right align columns and round to 2 decimal places
accessibleTables::format_data(
  wb,
  "Patient_Identification",
  c(
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "AA",
    "AB",
    "AC",
    "AD",
    "AE",
    "AF",
    "AG",
    "AH",
    "AI",
    "AJ"
  ),
  "right",
  "0.00"
)

# national Total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$national_total,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


# national chemical substance

accessibleTables::write_sheet(
  wb,
  "National_Chemical_Substance",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by BNF chemical substance and identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$national_chem_substance,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("H", "I"),
                              "right",
                              "#,##0")

# right align column J and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("J"),
                              "right",
                              "#,##0.00")

# ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  paste0(config$publication_table_title, " - Quarterly totals by ICB"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$icb,
  14
)

# left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

# right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

# right align column N and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

# gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  paste0(config$publication_table_title, " - Quarterly totals by gender"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$gender,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

# age band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  paste0(config$publication_table_title, " - Quarterly totals by age band"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$ageband,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

# age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly totals by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$age_gender,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

# right align column J and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

# patients per 1000 population by age and gender

accessibleTables::write_sheet(
  wb,
  "Population_by_Age_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly population totals split by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "5. ONS population estimates for 2023/2024 were not available prior to publication. ONS population estimates taken from https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales",
    "6. Patients per 1,000 population is an age-gender specific rate. These rates should only be analysed at the level at which they are presented and should not be used to compare across BNF sections.",
    "7. BNF section 0404 has relatively lower patient identification rates, with the most recent at 89.1% as of Q2 2023/24. This may result in an under-estimate of the number of patients receiving prescribing, and a corresponding under-estimate of patients per 1,000 population."
  ),
  quarterly_0404$pat_per_1000_pop,
  14
)

# left align columns A to H
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("A", "B", "C", "D", "E", "F", "G", "H"),
                              "left",
                              "")

# right align columns I and J and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("I", "J"),
                              "right",
                              "#,##0")

# right align column K and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("K"),
                              "right",
                              "#,##0.00")

# IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  paste0(config$publication_table_title, " - Quarterly totals by IMD"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$imd,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

# prescribing in children

accessibleTables::write_sheet(
  wb,
  "Presc_in_Children",
  paste0(
    config$publication_table_title,
    " - Prescribing in adults and children, age bands 17 and under to 18 and over"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$prescribing_in_children,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "Presc_in_Children",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Presc_in_Children",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Presc_in_Children",
                              c("H"),
                              "right",
                              "#,##0.00")

# monthly section data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF section"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$monthly_section,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("I"),
                              "right",
                              "#,##0.00")

# monthly chemical substance data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Chemical_Substance",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0404$monthly_chem_substance,
  14
)

# left align columns A to J
accessibleTables::format_data(
  wb,
  "Monthly_Chemical_Substance",
  c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
  "left",
  ""
)

# right align columns K and L and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("K", "L"),
                              "right",
                              "#,##0")

# right align column M and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("M"),
                              "right",
                              "#,##0.00")

# average items per patient monthly chemical substance data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Average_Items_per_Patient",
  paste0(
    config$publication_table_title,
    " - Average items per patient by month and BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "4. These averages refer to the mean. Average items per patient and average Net Ingredient Cost (NIC) per patient have only been calculated using items and costs associated with the identified patient flag. More information can be found in the Metadata sheet."
  ),
  quarterly_0404$avg_per_pat,
  14
)

# left align columns A to J
accessibleTables::format_data(
  wb,
  "Average_Items_per_Patient",
  c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
  "left",
  ""
)

# right align columns K and L and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("K", "L"),
                              "right",
                              "#,##0")

# right align column M, N, and O and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("M", "N", "O"),
                              "right",
                              "#,##0.00")

# create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0404 Central nervous system (CNS) stimulants and drugs used for ADHD",
  config$cover_sheet_sub_title,
  paste0("Publication date: ", config$publication_date),
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: Patient Identification Rates",
    "Table 2: National Total",
    "Table 3: National Chemical Substance",
    "Table 4: ICB",
    "Table 5: Gender",
    "Table 6: Age Band",
    "Table 7: Age Band and Gender",
    "Table 8 Population by Age Band and Gender",
    "Table 9: Indices of Deprivation (IMD)",
    "Table 10: Prescribing in Children",
    "Table 11: Monthly Section",
    "Table 12: Monthly Chemical Substance",
    "Table 13: Average Items per Identified Patient"
    
  ),
  c("Metadata", sheetNames)
)

# save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0404_Sep_23.xlsx",
                       overwrite = TRUE)

# 0411 Drugs for dementia - quarterly and monthly

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "National_Chemical_Substance",
  "ICB",
  "Gender",
  "Age_Band",
  "Age_Band_and_Gender",
  "Population_by_Age_Gender",
  "IMD",
  "Monthly_Section",
  "Monthly_Chemical_Substance",
  "Average_Items_per_Patient"
)

wb <- accessibleTables::create_wb(sheetNames)

# create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "BNF Section Name",
  "BNF Section Code",
  "Identified Patient Flag",
  "Total Identified Patients",
  "Total Items",
  "Total Net Ingredient Cost (GBP)",
  "Financial Year",
  "Financial Quarter",
  "Year Month",
  "BNF Paragraph Name",
  "BNF Paragraph Code",
  "ICB Name",
  "ICB Code",
  "BNF Chemical Substance Name",
  "BNF Chemical Substance Code",
  "Patient Gender",
  "Age Band",
  "IMD Quintile",
  "Average Items per Patient",
  "Average NIC per Patient (GBP)",
  "Mid-Year England Population Estimate",
  "Mid-Year Population Year",
  "Patients per 1,000 Population"
)

meta_descs <-
  c(
    "The name given to a British National Formulary (BNF) section. This is the next broadest grouping of The BNF Therapeutical classification system after chapter.",
    "The unique code used to refer to the British National Formulary (BNF) section.",
    "This shows where an item has been attributed to an NHS number that has been verified by the Personal Demographics Service.",
    "Where patients are identified via the flag, the number of patients that the data corresponds to. This will always be 0 where 'Identified Patient' = N.",
    "The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
    "Total Net Ingredient Cost is the amount that would be paid using the basic price of the prescribed drug or appliance and the quantity prescribed. Sometimes called the 'Net Ingredient Cost' (NIC). The basic price is given either in the Drug Tariff or is determined from prices published by manufacturers, wholesalers or suppliers. Basic price is set out in Parts 8 and 9 of the Drug Tariff. For any drugs or appliances not in Part 8, the price is usually taken from the manufacturer, wholesaler or supplier of the product. This is given in GBP (£).",
    "The financial year to which the data belongs.",
    "The financial quarter to which the data belongs.",
    "The year and month to which the data belongs, denoted in YYYYMM format.",
    "The name given to the British National Formular (BNF) paragraph. This level of grouping of the BNF Therapeutical classification system sits below BNF section.",
    "The unique code used to refer to the British National Formulary (BNF) paragraph.",
    "The name given to the Integrated Care Board (ICB) that a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
    "The unique code used to refer to an Integrated Care Board (ICB).",
    "The name of the main active ingredient in a drug. Appliances do not hold a chemical substance, but instead inherit the corresponding BNF section. Determined by the British National Formulatory (BNF) for drugs, or the NHS BSA for appliances. For example, Amoxicillin.",
    "The unique code used to refer to the British National Formulary (BNF) chemical substance.",
    "The gender of the patient as at the time the prescription was processed. Please see the detailed Background Information and Methodology notice released with this publication for further information.",
    "The age band of the patient as of the 30th September of the corresponding financial year the drug was prescribed.",
    "The IMD quintile of the patient, based on the patient's postcode, where '1' is the 20% of areas with the highest deprivation score in the Index of Multiple Deprivation (IMD) from the English Indices of Deprivation 2019, and '5' is the 20% of areas with the lowest IMD deprivation score. The IMD quintile has been recorded as 'Unknown' where the items are attributed to an unidentified patient, or where we have been unable to match the patient postcode to a postcode in the National Statistics Postcode Lookup (NSPL).",
    "The total number of items divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean.",
    "The total Net Ingredient Cost divided by the total number of identified patients, by month and chemical substance. This only uses items that have been attributed to patients via the identified patient flag. This average refers to the mean, and is given in GBP (£).",
    "The population estimate for the corresponding Mid-Year Population Year.",
    "The year in which population estimates were taken, required due to the presentation of this data in financial year format.",
    "The number of identified patients by age band and gender divided by mid-year population of the same age band and gender group in England, multiplied by 1,000. Only identified patients with a known gender and age band are included. This is calculated by (Total Identified Patients / Mid-Year England Population Estimate) * 1000."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)

# patient identification

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Patient_Identification",
  paste0(
    config$publication_table_title,
    " - Proportion of items for which an NHS number was recorded (%)"
  ),
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0411$patient_id,
  42
)

# left align columns A to B
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

# right align columns and round to 2 decimal places
accessibleTables::format_data(
  wb,
  "Patient_Identification",
  c(
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "AA",
    "AB",
    "AC",
    "AD",
    "AE",
    "AF",
    "AG",
    "AH",
    "AI",
    "AJ"
  ),
  "right",
  "0.00"
)

# national Total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0411$national_total,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


# national chemical substance

accessibleTables::write_sheet(
  wb,
  "National_Chemical_Substance",
  paste0(
    config$publication_table_title,
    " - Quarterly totals split by BNF chemical substance and identified patients"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0411$national_chem_substance,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("H", "I"),
                              "right",
                              "#,##0")

# right align column J and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("J"),
                              "right",
                              "#,##0.00")

# ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  paste0(config$publication_table_title, " - Quarterly totals by ICB"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0411$icb,
  14
)

# left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

# right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

# right align column N and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

# gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  paste0(config$publication_table_title, " - Quarterly totals by gender"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0411$gender,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

# age band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  paste0(config$publication_table_title, " - Quarterly totals by age band"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0411$ageband,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

# age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly totals by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0411$age_gender,
  14
)

# left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

# right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

# right align column J and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

# patients per 1000 population by age and gender

accessibleTables::write_sheet(
  wb,
  "Population_by_Age_Gender",
  paste0(
    config$publication_table_title,
    " - Quarterly population totals split by age band and gender"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "5. ONS population estimates for 2023/2024 were not available prior to publication. ONS population estimates taken from https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales",
    "6. Patients per 1,000 population is an age-gender specific rate. These rates should only be analysed at the level at which they are presented and should not be used to compare across BNF sections."
  ),
  quarterly_0411$pat_per_1000_pop,
  14
)

# left align columns A to H
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("A", "B", "C", "D", "E", "F", "G", "H"),
                              "left",
                              "")

# right align columns I and J and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("I", "J"),
                              "right",
                              "#,##0")

# right align column K and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Population_by_Age_Gender",
                              c("K"),
                              "right",
                              "#,##0.00")

# IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  paste0(config$publication_table_title, " - Quarterly totals by IMD"),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile.",
    "4. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0411$imd,
  14
)

# left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

# right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

# right align column H and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

# monthly section data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF section"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0411$monthly_section,
  14
)

# left align columns A to F
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

# right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("G", "H"),
                              "right",
                              "#,##0")

# right align column I and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Section",
                              c("I"),
                              "right",
                              "#,##0.00")

# monthly chemical substance data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Chemical_Substance",
  paste0(
    config$publication_table_title,
    " - Monthly totals by BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  quarterly_0411$monthly_chem_substance,
  14
)

# left align columns A to H
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("A", "B", "C", "D", "E", "F", "G", "H"),
                              "left",
                              "")

# right align columns I and J and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("I", "J"),
                              "right",
                              "#,##0")

# right align column K and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Monthly_Chemical_Substance",
                              c("K"),
                              "right",
                              "#,##0.00")

# average items per patient monthly chemical substance data

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Average_Items_per_Patient",
  paste0(
    config$publication_table_title,
    " - Average items per patient by month and BNF chemical substance"
  ),
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing fewer than 5 patients or items. These cells will appear blank.",
    "3. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "4. These averages refer to the mean. Average items per patient and average Net Ingredient Cost (NIC) per patient have only been calculated using items and costs associated with the identified patient flag. More information can be found in the Metadata sheet."
  ),
  quarterly_0411$avg_per_pat,
  14
)

# left align columns A to J
accessibleTables::format_data(
  wb,
  "Average_Items_per_Patient",
  c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
  "left",
  ""
)

# right align columns K and L and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("K", "L"),
                              "right",
                              "#,##0")

# right align column M, N, and O and round to 2 decimal places with thousand separator
accessibleTables::format_data(wb,
                              "Average_Items_per_Patient",
                              c("M", "N", "O"),
                              "right",
                              "#,##0.00")

# create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0411 Drugs for dementia",
  config$cover_sheet_sub_title,
  paste0("Publication date: ", config$publication_date),
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: Patient Identification Rates",
    "Table 2: National Total",
    "Table 3: National Chemical Substance",
    "Table 4: ICB",
    "Table 5: Gender",
    "Table 6: Age Band",
    "Table 7: Age Band and Gender",
    "Table 8: Population by Age Band and Gender",
    "Table 9: Indices of Deprivation (IMD)",
    "Table 10: Monthly Section",
    "Table 11: Monthly Chemical Substance",
    "Table 12: Average Items per Identified Patient"
  ),
  c("Metadata", sheetNames)
)

# save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0411_Sep_23.xlsx",
                       overwrite = TRUE)

# 6. Charts and figures --------------------------------------------------------

# chart data for use in markdown

# table 1 patient ID rates
table_1_data <- capture_rate_extract_quarter |>
  dplyr::select(
    `BNF Section Name`,
    `BNF Section Code`,
    `2022/2023 Q3`,
    `2022/2023 Q4`,
    `2023/2024 Q1`,
    `2023/2024 Q2`
  ) |>
  dplyr::mutate(across(where(is.numeric), round, 1)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

table_1_data_format <- capture_rate_extract_quarter |>
  dplyr::select(
    `BNF Section Name`,
    `BNF Section Code`,
    `2022/2023 Q3`,
    `2022/2023 Q4`,
    `2023/2024 Q1`,
    `2023/2024 Q2`
  ) |>
  dplyr::mutate(across(where(is.numeric), round, 1)) |>
  mutate(across(where(is.numeric), round, 2)) |>
  mutate(across(where(is.numeric), format, nsmall = 2)) |>
  mutate(across(contains("20"), ~ paste0(.x, "%")))

table_1 <- table_1_data_format |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))

# table 1 patient ID rates by section
# antidepressants 0403
table_1_data_0403 <- capture_rate_extract_quarter |>
  dplyr::select(
    `BNF Section Name`,
    `BNF Section Code`,
    `2022/2023 Q3`,
    `2022/2023 Q4`,
    `2023/2024 Q1`,
    `2023/2024 Q2`
  ) |>
  dplyr::filter(`BNF Section Code` == "0403") |>
  dplyr::mutate(across(where(is.numeric), round, 1)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

table_1_0403 <- table_1_data_0403 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))

# hypnotics and anxiolytics 0401
table_1_data_0401 <- capture_rate_extract_quarter |>
  dplyr::select(
    `BNF Section Name`,
    `BNF Section Code`,
    `2022/2023 Q3`,
    `2022/2023 Q4`,
    `2023/2024 Q1`,
    `2023/2024 Q2`
  ) |>
  dplyr::filter(`BNF Section Code` == "0401") |>
  dplyr::mutate(across(where(is.numeric), round, 1)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

table_1_0401 <- table_1_data_0401 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))

# drugs used in psychoses and related disorders 0402
table_1_data_0402 <- capture_rate_extract_quarter |>
  dplyr::select(
    `BNF Section Name`,
    `BNF Section Code`,
    `2022/2023 Q3`,
    `2022/2023 Q4`,
    `2023/2024 Q1`,
    `2023/2024 Q2`
  ) |>
  dplyr::filter(`BNF Section Code` == "0402") |>
  dplyr::mutate(across(where(is.numeric), round, 1)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

table_1_0402 <- table_1_data_0402 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))

# CNS stimulants and drugs used for ADHD 0404
table_1_data_0404 <- capture_rate_extract_quarter |>
  dplyr::select(
    `BNF Section Name`,
    `BNF Section Code`,
    `2022/2023 Q3`,
    `2022/2023 Q4`,
    `2023/2024 Q1`,
    `2023/2024 Q2`
  ) |>
  dplyr::filter(`BNF Section Code` == "0404") |>
  dplyr::mutate(across(where(is.numeric), round, 1)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

table_1_0404 <- table_1_data_0404 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))
# drugs for dementia 0411
table_1_data_0411 <- capture_rate_extract_quarter |>
  dplyr::select(
    `BNF Section Name`,
    `BNF Section Code`,
    `2022/2023 Q3`,
    `2022/2023 Q4`,
    `2023/2024 Q1`,
    `2023/2024 Q2`
  ) |>
  dplyr::filter(`BNF Section Code` == "0411") |>
  dplyr::mutate(across(where(is.numeric), round, 1)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

table_1_0411 <- table_1_data_0411 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))

# figure 1 quarterly antidepressant items and patients
figure_1_data_0403 <- quarterly_0403$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_1_0403 <- figure_1_data_0403 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 2 quarterly antidepressant net ingredient cost
figure_2_data_0403 <- quarterly_0403$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(`Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
                   .groups = "drop") |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_2_0403 <- figure_2_data_0403 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Cost (GBP)",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 3 monthly antidepressant items and patients
figure_3_data_0403 <- quarterly_0403$monthly_section |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Year Month`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    MONTH_INDEX = dplyr::row_number(),
    MONTH_START = as.Date(paste0(`Year Month`, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START)
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_3_0403 <- figure_3_data_0403 |>
  nhsbsaVis::group_chart_hc(
    x = MONTHSTART,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Month",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(
    type = "datetime",
    dateTimeLabelFormats = list(month = "%b %y"),
    title = list(text = "Month")
  )

# figure 1 quarterly hypnotics and anxiolytics items and patients
figure_1_data_0401 <- quarterly_0401$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_1_0401 <- figure_1_data_0401 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 2 quarterly hypnotics and anxiolytics net ingredient cost
figure_2_data_0401 <- quarterly_0401$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(`Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
                   .groups = "drop") |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_2_0401 <- figure_2_data_0401 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Cost (GBP)",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 3 monthly hypnotics and anxiolytics items and patients
figure_3_data_0401 <- quarterly_0401$monthly_section |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Year Month`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    MONTH_INDEX = dplyr::row_number(),
    MONTH_START = as.Date(paste0(`Year Month`, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START)
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_3_0401 <- figure_3_data_0401 |>
  nhsbsaVis::group_chart_hc(
    x = MONTHSTART,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Month",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(
    type = "datetime",
    dateTimeLabelFormats = list(month = "%b %y"),
    title = list(text = "Month")
  )

# figure 1 quarterly antipsychotic items and patients
figure_1_data_0402 <- quarterly_0402$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_1_0402 <- figure_1_data_0402 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 2 quarterly antipsychotic net ingredient cost
figure_2_data_0402 <- quarterly_0402$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(`Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
                   .groups = "drop") |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_2_0402 <- figure_2_data_0402 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Cost (GBP)",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 3 monthly antipsychotic items and patients
figure_3_data_0402 <- quarterly_0402$monthly_section |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Year Month`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    MONTH_INDEX = dplyr::row_number(),
    MONTH_START = as.Date(paste0(`Year Month`, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START)
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_3_0402 <- figure_3_data_0402 |>
  nhsbsaVis::group_chart_hc(
    x = MONTHSTART,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Month",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(
    type = "datetime",
    dateTimeLabelFormats = list(month = "%b %y"),
    title = list(text = "Month")
  )

# figure 1 quarterly CNS ADHD items and patients
figure_1_data_0404 <- quarterly_0404$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_1_0404 <- figure_1_data_0404 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 2 quarterly CNS ADHD net ingredient cost
figure_2_data_0404 <- quarterly_0404$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(`Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
                   .groups = "drop") |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_2_0404 <- figure_2_data_0404 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Cost (GBP)",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 3 monthly CNS ADHD items and patients
figure_3_data_0404 <- quarterly_0404$monthly_section |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Year Month`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    MONTH_INDEX = dplyr::row_number(),
    MONTH_START = as.Date(paste0(`Year Month`, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START)
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_3_0404 <- figure_3_data_0404 |>
  nhsbsaVis::group_chart_hc(
    x = MONTHSTART,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Month",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(
    type = "datetime",
    dateTimeLabelFormats = list(month = "%b %y"),
    title = list(text = "Month")
  )

# figure 1 quarterly drugs for dementia items and patients
figure_1_data_0411 <- quarterly_0411$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_1_0411 <- figure_1_data_0411 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 2 quarterly drugs for dementia net ingredient cost
figure_2_data_0411 <- quarterly_0411$national_total |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(`Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
                   .groups = "drop") |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_2_0411 <- figure_2_data_0411 |>
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_QUARTER,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Financial quarter",
    yLab = "Cost (GBP)",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(labels = list(step = 2,
                         rotation = -45))

# figure 3 monthly drugs for dementia items and patients
figure_3_data_0411 <- quarterly_0411$monthly_section |>
  #added financial year filter now only using rolling years
  dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017", "2017/2018")) |>
  dplyr::group_by(`Financial Year`,
                  `Year Month`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Prescribed items` = sum(`Total Items`),
    `Identified patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    MONTH_INDEX = dplyr::row_number(),
    MONTH_START = as.Date(paste0(`Year Month`, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START)
  ) |>
  tidyr::pivot_longer(
    cols = c(`Identified patients`, `Prescribed items`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure)) |>
  rename_with( ~ gsub(" ", "_", toupper(gsub(
    "[^[:alnum:] ]", "", .
  ))), everything())

figure_3_0411 <- figure_3_data_0411 |>
  nhsbsaVis::group_chart_hc(
    x = MONTHSTART,
    y = VALUE,
    group = MEASURE,
    type = "line",
    marker = FALSE,
    dlOn = FALSE,
    xLab = "Month",
    yLab = "Volume",
    title = ""
  ) |>
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE) |>
  hc_legend(enabled = TRUE) |>
  hc_xAxis(
    type = "datetime",
    dateTimeLabelFormats = list(month = "%b %y"),
    title = list(text = "Month")
  )

# figure 1 antidepressants model data
figure_1_data_covid <- predictions_0403 |>
  dplyr::filter(YEAR_MONTH > 202002)
library(highcharter)

figure_1_covid <- figure_1_data_covid |>
  covid_chart_hc(title = "")

# figure 2 hypnotics anxiolytics model data
figure_2_data_covid <- predictions_0401 |>
  dplyr::filter(YEAR_MONTH > 202002)

figure_2_covid <- figure_2_data_covid |>
  covid_chart_hc(title = "")

# figure 3 antipsychotics model data
figure_3_data_covid <- predictions_0402 |>
  dplyr::filter(YEAR_MONTH > 202002)

figure_3_covid <- figure_3_data_covid |>
  covid_chart_hc(title = "")

# figure 4 CNS ADHD model data
figure_4_data_covid <- predictions_0404 |>
  dplyr::filter(YEAR_MONTH > 202002)

figure_4_covid <- figure_4_data_covid |>
  covid_chart_hc(title = "")

# figure 5 dementia model data
figure_5_data_covid <- predictions_0411 |>
  filter(YEAR_MONTH > 202002)

figure_5_covid <- figure_5_data_covid |>
  covid_chart_hc(title = "")

# 7. Render outputs ------------------------------------------------------------

rmarkdown::render(
  "mumh_quarterly_sep23_overview.Rmd",
  output_format = "html_document",
  output_file = "outputs/mumh_quarterly_sep23_overview.html"
)
rmarkdown::render(
  "mumh_quarterly_sep23_overview.Rmd",
  output_format = "word_document",
  output_file = "outputs/mumh_quarterly_sep23_overview.docx"
)

rmarkdown::render(
  "mumh_quarterly_sep23_0403.Rmd",
  output_format = "html_document",
  output_file = "outputs/mumh_quarterly_sep23_0403.html"
)
rmarkdown::render(
  "mumh_quarterly_sep23_0403.Rmd",
  output_format = "word_document",
  output_file = "outputs/mumh_quarterly_sep23_0403.docx"
)
rmarkdown::render(
  "mumh_quarterly_sep23_0401.Rmd",
  output_format = "html_document",
  output_file = "outputs/mumh_quarterly_sep23_0401.html"
)
rmarkdown::render(
  "mumh_quarterly_sep23_0401.Rmd",
  output_format = "word_document",
  output_file = "outputs/mumh_quarterly_sep23_0401.docx"
)
rmarkdown::render(
  "mumh_quarterly_sep23_0402.Rmd",
  output_format = "html_document",
  output_file = "outputs/mumh_quarterly_sep23_0402.html"
)
rmarkdown::render(
  "mumh_quarterly_sep23_0402.Rmd",
  output_format = "word_document",
  output_file = "outputs/mumh_quarterly_sep23_0402.docx"
)
rmarkdown::render(
  "mumh_quarterly_sep23_0404.Rmd",
  output_format = "html_document",
  output_file = "outputs/mumh_quarterly_sep23_0404.html"
)
rmarkdown::render(
  "mumh_quarterly_sep23_0404.Rmd",
  output_format = "word_document",
  output_file = "outputs/mumh_quarterly_sep23_0404.docx"
)
rmarkdown::render(
  "mumh_quarterly_sep23_0411.Rmd",
  output_format = "html_document",
  output_file = "outputs/mumh_quarterly_sep23_0411.html"
)
rmarkdown::render(
  "mumh_quarterly_sep23_0411.Rmd",
  output_format = "word_document",
  output_file = "outputs/mumh_quarterly_sep23_0411.docx"
)

rmarkdown::render(
  "mumh_quarterly_sep23_covid.Rmd",
  output_format = "html_document",
  output_file = "outputs/mumh_quarterly_sep23_model.html"
)
rmarkdown::render(
  "mumh_quarterly_sep23_covid.Rmd",
  output_format = "word_document",
  output_file = "outputs/mumh_quarterly_sep23_model.docx"
)

rmarkdown::render("mumh_background_sep23.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/mumh_background_sep23.html")
rmarkdown::render("mumh_background_sep23.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/mumh_background_sep23.docx")