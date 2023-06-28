#pipeline.R
#this script provides the code to run the reproducible analytical pipeline
#and produce the Medicines Used in Mental Health (MUMH) publication

#clear environment
rm(list = ls())

#source functions - commented out while I check if required/ add functions to folder
#this is only a temporary step until all functions are built into packages
source("./functions/functions.R")

#1. Setup and package installation
#load GITHUB_KEY if available in environment or enter if not

if (Sys.getenv("GITHUB_PAT") == "") {
  usethis::edit_r_environ()
  stop(
    "You need to set your GITHUB_PAT = YOUR PAT KEY in the .Renviron file which pops up. Please restart your R Studio after this and re-run the pipeline."
  )
}

#load GITHUB_KEY if available in environment or enter if not

#check if Excel outputs are required
makeSheet <- menu(c("Yes", "No"),
                  title = "Do you wish to generate the Excel outputs?")

#install nhsbsaUtils package first to use function check_and_install_packages()
devtools::install_github("nhsbsa-data-analytics/nhsbsaUtils",
                         auth_token = Sys.getenv("GITHUB_PAT"), force = TRUE)

library(nhsbsaUtils)

#1. install required packages
#double check required packages once full pipeline built eg. if maps used
req_pkgs <- c("broom",
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
              "tcltk",
              "tidyr",
              "tidyverse",
              "vroom",
              "yaml")

#library/install packages as required using nhsbsaUtils
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

#2. data import

con <- con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP",
  username = rstudioapi::showPrompt(title = "Username", message = "Username"),
  password = rstudioapi::askForPassword()
)

#get max fy from mumh fact table
#ADD SCHEMA AND TABLE NAME ONCE KNOWN
#max_dw_fy <- dplyr::tbl(con,
#                        from = dbplyr::in_schema()) |>
#  dplyr::filter(MONTH_TYPE %in% c("FY")) |>
# dplyr::select(YEAR_DESC) |>
#  dplyr::filter(YEAR_DESC == max(YEAR_DESC, na.rm = TRUE)) |>
#  distinct() |>
#  collect() |>
#  pull()

log_print("Max DWH FY pulled", hide_notes = TRUE)
log_print(max_dw_fy, hide_notes = TRUE)

#add max calender year code to get max year for use in ons_national_pop() function
#in externaldata package
#max_dw_cy <- dplyr::tbl(con,
#                        from = dbplyr::in_schema()) |>
#  dplyr::filter(MONTH_TYPE %in% c("FY")) |>
#  dplyr::select(YEAR_DESC) |>
#  dplyr::filter(YEAR_DESC == max(YEAR_DESC, na.rm = TRUE)) |>
#  distinct() |>
#  collect() |>
#  pull()

max_dw_cy <- 2023

#get max financial quarter from mumh fact table
#ADD SCHEMA AND TABLE NAME ONCE KNOWN
# AND QUARTER COLUMN NAME TO REPLACE CY EXAMPLE IN CODE
#max_dw_quart <- dplyr::tbl(con,
#                        from = dbplyr::in_schema()) |>
#  dplyr::filter(MONTH_TYPE %in% c("CY")) |>
#  dplyr::select(YEAR_DESC) |>
#  dplyr::filter(YEAR_DESC == max(YEAR_DESC, na.rm = TRUE)) |>
#  distinct() |>
#  collect() |>
#  pull()

#log_print("Max DWH quarter pulled", hide_notes = TRUE)
#log_print(max_dw_quart, hide_notes = TRUE)

# only get new data if max month in dwh is greater than that in most recent data

#  DBI::dbDisconnect(con)
  
#  save_data(mumh_quarterly, dir = "Y:/Official Stats/MUMH", filename = "mumh_quarterly")
  
#  save_data(mumh_monthly, dir = "Y:/Official Stats/MUMH", filename = "mumh_monthly")
  
#  save_data(mumh_model, dir = "Y:/Official Stats/MUMH", filename = "mumh_model")
# }  

# 3. Load latest data
# for 2022/23 data run with new methodology so not using saved data
# TO DO: add code to load latest data before next release

# 4. load reference data  ---------

#dispensing days data
#use latest year of financial year period
dispensing_days_data <- nhsbsaUtils::dispensing_days(2023)
log_print("Dispensing days data loaded", hide_notes = TRUE)

#map data
icb_geo_data <- nhsbsaExternalData::icb_geo_data()
log_print("Geo data loaded", hide_notes = TRUE)

#lookups
icb_lsoa_lookup <- nhsbsaExternalData::icb_lsoa_lookup()
log_print("Lookup data loaded", hide_notes = TRUE)

#population data
imd_population <- nhsbsaExternalData::imd_population()
lsoa_population_overall <-
  nhsbsaExternalData::lsoa_population(group = "Overall")
en_ons_national_pop <-
  nhsbsaExternalData::ons_national_pop(year = c(2014:as.numeric(max_dw_cy)), area = "ENPOP")

#build icb population lookup
icb_pop <- icb_lsoa_lookup |>
  dplyr::left_join(lsoa_population_overall,
                   by = c("LSOA_CODE" = "LSOA_CODE")) |>
  dplyr::group_by(ICB_CODE, ICB_NAME, ICB_LONG_CODE) |>
  dplyr::summarise(POP = sum(POP, na.rm = TRUE),
                   .groups = "drop")

log_print("Population data loaded", hide_notes = TRUE)


#3. raw data structure

#financial year data extracts

capture_rate_extract_year <- capture_rate_extract_period(con = con,
                                                             period_type = "year")
national_extract_year <- national_extract_period(con = con,
                                                    period_type = "year")
population_extract_year <- population_extract()
paragraph_extract_year <- paragraph_extract_period(con = con,
                                                      period_type = "year")
chem_sub_extract_year <- chem_sub_extract_period(con = con,
                                                    period_type = "year")
icb_extract_year <- icb_extract_period(con = con,
                                          period_type = "year")
ageband_data_year <- ageband_extract_period(con = con,
                                            period_type = "year")
gender_extract_year <- gender_extract_period(con = con,
                                                period_type = "year")
age_gender_extract_year <- age_gender_extract_period(con = con,
                                                        period_type = "year")
imd_extract_year <- imd_extract_period(con = con,
                                          period_type = "year")
#child_adult_extract_year <- child_adult_extract(con = con,
#                                                period_type = "year")

log_print("Annual extracts pulled", hide_notes = TRUE)

#quarterly data extracts

capture_rate_extract__quarter <- capture_rate_extract_period(con = con,
                                                             period_type = "quarter")
national_extract_quarter <- national_extract_period(con = con,
                                                    period_type = "quarter")
paragraph_extract_quarter <- paragraph_extract_period(con = con,
                                                      period_type = "quarter")
chem_sub_extract_quarter <- chem_sub_extract_period(con = con,
                                                    period_type = "quarter")
icb_extract_quarter <- icb_extract_period(con = con,
                                          period_type = "quarter")
ageband_data_quarter <- ageband_extract_period(con = con,
                                               period_type = "quarter")
gender_extract_quarter <- gender_extract_period(con = con,
                                                period_type = "quarter")
age_gender_extract_quarter <- age_gender_extract_period(con = con,
                                                        period_type = "quarter")
imd_extract_quarter <- imd_extract_period(con = con,
                                          period_type = "quarter")
#child_adult_extract_quarter <- child_adult_extract(con = con,
#                                                period_type = "quarter")

log_print("Quarterly extracts pulled", hide_notes = TRUE)

#monthly data extracts

national_extract_monthly <- national_extract_period(con = con,
                                                    period_type = "month")
paragraph_extract_monthly <- paragraph_extract_period(con = con,
                                                      period_type = "month")

log_print("Monthly extracts pulled", hide_notes = TRUE)


#aggregations and analysis

# 0401 Hypnotics and anxiolytics workbook - annual

annual_0401 <- list()

annual_0401$patient_id <- capture_rate_extract_year %>%
  dplyr::filter(`BNF Section Code` == "0401") 

annual_0401$national_total <- national_extract_year %>%
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$national_population <- population_extract_year %>%
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$national_paragraph <- paragraph_extract_year %>%
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$icb <-  icb_extract_year %>%
  apply_sdc(rounding = F) %>%
  dplyr::select( `Financial Year`,
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
                 `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$gender <- gender_extract_year %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0401") 

annual_0401$ageband <- ageband_data_year %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$age_gender <- age_gender_extract_year %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$imd <- imd_extract_year %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0401")




#4. chart data

#5. analysis

#6. covid model

#7. tables

#8. render markdown