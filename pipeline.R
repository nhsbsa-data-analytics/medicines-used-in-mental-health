#pipeline.R
#this script provides the code to run the reproducible analytical pipeline
#and produce the Medicines Used in Mental Health (MUMH) publication

#clear environment
rm(list = ls())

#source functions
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

#get max fy from MUMH fact table
max_dw_fy <- dplyr::tbl(con,
                        from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307")) |>
  dplyr::select(FINANCIAL_YEAR) |>
  dplyr::filter(FINANCIAL_YEAR == max(FINANCIAL_YEAR, na.rm = TRUE)) |>
  distinct() |>
  collect() |>
  pull()

log_print("Max DWH FY pulled", hide_notes = TRUE)
log_print(max_dw_fy, hide_notes = TRUE)

#add max calender year code to get max year for use in ons_national_pop() function
#in externaldata package
max_dw_cy <- substr(max_dw_fy, 6, 9)

log_print("Max DWH CY pulled", hide_notes = TRUE)
log_print(max_dw_cy, hide_notes = TRUE)

#get max financial quarter from mumh fact table
max_dw_qu <- dplyr::tbl(con,
                        from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307")) |>
  dplyr::select(FINANCIAL_QUARTER) |>
  dplyr::filter(FINANCIAL_QUARTER == max(FINANCIAL_QUARTER, na.rm = TRUE)) |>
  distinct() |>
  collect() |>
  pull()

log_print("Max DWH quarter pulled", hide_notes = TRUE)
log_print(max_dw_qu, hide_notes = TRUE)

#get max year month from mumh fact table
max_dw_ym <- dplyr::tbl(con,
                        from = dbplyr::in_schema("MAWIL", "MUMH_FACT_202307")) |>
dplyr::select(YEAR_MONTH) |>
dplyr::filter(YEAR_MONTH == max(YEAR_MONTH, na.rm = TRUE)) |>
distinct() |>
collect() |>
pull()

log_print("Max DWH year month pulled", hide_notes = TRUE)
log_print(max_dw_ym, hide_notes = TRUE)

# only get new data if max month in dwh is greater than that in most recent data
  
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
child_adult_extract_year <- child_adult_extract(con = con,
                                                period_type = "year")

log_print("Annual extracts pulled", hide_notes = TRUE)

#quarterly data extracts

capture_rate_extract_quarter <- capture_rate_extract_period(con = con,
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
child_adult_extract_quarter <- child_adult_extract(con = con,
                                                period_type = "quarter")

log_print("Quarterly extracts pulled", hide_notes = TRUE)

#monthly data extracts

national_extract_monthly <- national_extract_period(con = con,
                                                    period_type = "month")
paragraph_extract_monthly <- paragraph_extract_period(con = con,
                                                      period_type = "month")
chem_sub_extract_monthly <- chem_sub_extract_period(con = con,
                                                    period_type = "month")
age_gender_extract_month <- age_gender_extract_period(con = con,
                                                      period_type = "month")

log_print("Monthly extracts pulled", hide_notes = TRUE)

#disconnect from data warehouse once all extracts pulled

DBI::dbDisconnect(con)

#aggregations and analysis

# 0401 Hypnotics and anxiolytics workbook - annual

annual_0401 <- list()

annual_0401$patient_id <- capture_rate_extract_year |>
  dplyr::filter(`BNF Section Code` == "0401") 

annual_0401$national_total <- national_extract_year |>
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$national_population <- population_extract_year |>
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$national_paragraph <- paragraph_extract_year |>
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$icb <-  icb_extract_year |>
  apply_sdc(rounding = F) |>
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
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$gender <- gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401") 

annual_0401$ageband <- ageband_data_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$age_gender <- age_gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401")

annual_0401$imd <- imd_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401")


# 0402 Drugs used in psychoses and related disorders - annual


annual_0402 <- list()

annual_0402$patient_id <- capture_rate_extract_year |>
  dplyr::filter(`BNF Section Code` == "0402") 

annual_0402$national_total <- national_extract_year |>
  dplyr::filter(`BNF Section Code` == "0402")

annual_0402$national_population <- population_extract_year |>
  dplyr::filter(`BNF Section Code` == "0402")

annual_0402$national_paragraph <- paragraph_extract_year |>
  dplyr::filter(`BNF Section Code` == "0402")

annual_0402$icb <-  icb_extract_year |>
  apply_sdc(rounding = F) |>
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
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0402")

annual_0402$gender <- gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0402") 

annual_0402$ageband <- ageband_data_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0402")

annual_0402$age_gender <- age_gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0402")

annual_0402$imd <- imd_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0402")

# 0403 Antidepressants - annual

annual_0403 <- list()

annual_0403$patient_id <- capture_rate_extract_year |>
  dplyr::filter(`BNF Section Code` == "0403") 

annual_0403$national_total <- national_extract_year |>
  dplyr::filter(`BNF Section Code` == "0403")

annual_0403$national_population <- population_extract_year |>
  dplyr::filter(`BNF Section Code` == "0403")

annual_0403$national_paragraph <- paragraph_extract_year |>
  dplyr::filter(`BNF Section Code` == "0403")

annual_0403$icb <-  icb_extract_year |>
  apply_sdc(rounding = F) |>
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
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403")

annual_0403$gender <- gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403") 

annual_0403$ageband <- ageband_data_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403")

annual_0403$age_gender <- age_gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403")

annual_0403$imd <- imd_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403")

annual_0403$prescribing_in_children <- child_adult_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403")


# 0404 CNS stimulants and drugs used for ADHD - annual

annual_0404 <- list()

annual_0404$patient_id <- capture_rate_extract_year |>
  dplyr::filter(`BNF Section Code` == "0404") 

annual_0404$national_total <- national_extract_year |>
  dplyr::filter(`BNF Section Code` == "0404")

annual_0404$national_population <- population_extract_year |>
  dplyr::filter(`BNF Section Code` == "0404")

annual_0404$national_chem_substance <- chem_sub_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select( `Financial Year`,
                 `BNF Section Name`,
                 `BNF Section Code`,
                 `BNF Chemical Substance Name`,
                 `BNF Chemical Substance Code`,
                 `Identified Patient Flag`,
                 `Total Identified Patients` = `sdc_Total Identified Patients`,
                 `Total Items` = `sdc_Total Items`,
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404")

annual_0404$icb <-  icb_extract_year |>
  apply_sdc(rounding = F) |>
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
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404")

annual_0404$gender <- gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404") 

annual_0404$ageband <- ageband_data_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404")

annual_0404$age_gender <- age_gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404")

annual_0404$imd <- imd_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404")

annual_0404$prescribing_in_children <- child_adult_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404")  

# 0411 Drugs for dementia workbook - annual

annual_0411 <- list()

annual_0411$patient_id <- capture_rate_extract_year |>
  dplyr::filter(`BNF Section Code` == "0411") 

annual_0411$national_total <- national_extract_year |>
  dplyr::filter(`BNF Section Code` == "0411")

annual_0411$national_population <- population_extract_year |>
  dplyr::filter(`BNF Section Code` == "0411")

annual_0411$national_chem_substance <- chem_sub_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select( `Financial Year`,
                 `BNF Section Name`,
                 `BNF Section Code`,
                 `BNF Chemical Substance Name`,
                 `BNF Chemical Substance Code`,
                 `Identified Patient Flag`,
                 `Total Identified Patients` = `sdc_Total Identified Patients`,
                 `Total Items` = `sdc_Total Items`,
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411")


annual_0411$icb <-  icb_extract_year |>
  apply_sdc(rounding = F) |>
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
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411")

annual_0411$gender <- gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411") 

annual_0411$ageband <- ageband_data_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411")

annual_0411$age_gender <- age_gender_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411")

annual_0411$imd <- imd_extract_year |>
  apply_sdc(rounding = F) |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients` = `sdc_Total Identified Patients`,
    `Total Items` = `sdc_Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411")

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
  dplyr::select( `Financial Year`,
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
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$gender <- gender_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401") 

quarterly_0401$ageband <- ageband_data_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401")

quarterly_0401$age_gender <- age_gender_extract_quarter |>
  apply_sdc(rounding = F) |>
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
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
    `Total Net Ingredient Cost (GBP)`) |>
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
    `Total Net Ingredient Cost (GBP)`) |>
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
    `Total Net Ingredient Cost (GBP)`) |>
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
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401")

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
  dplyr::select( `Financial Year`,
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
                 `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$gender <- gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0402") 

quarterly_0402$ageband <- ageband_data_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$age_gender <- age_gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0402")

quarterly_0402$monthly_chem_substance <- chem_sub_extract_monthly %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0402")


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
  dplyr::select( `Financial Year`,
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
                 `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$gender <- gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0403") 

quarterly_0403$ageband <- ageband_data_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$age_gender <- age_gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0403")

quarterly_0403$prescribing_in_children <- child_adult_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0403")

###add chemical substance level in monthly tables if needed

quarterly_0403$monthly_chem_substance <- chem_sub_extract_monthly %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0403")

# 0404 CNS stimulants and drugs used for ADHD - quarterly

quarterly_0404 <- list()

quarterly_0404$patient_id <- capture_rate_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0404") 

quarterly_0404$national_total <- national_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$national_chem_substance <- chem_sub_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select( `Financial Year`,
                 `Financial Quarter`,
                 `BNF Section Name`,
                 `BNF Section Code`,
                 `BNF Chemical Substance Name`,
                 `BNF Chemical Substance Code`,
                 `Identified Patient Flag`,
                 `Total Identified Patients` = `sdc_Total Identified Patients`,
                 `Total Items` = `sdc_Total Items`,
                 `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$icb <-  icb_extract_quarter%>%
  apply_sdc(rounding = F) %>%
  dplyr::select( `Financial Year`,
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
                 `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$gender <- gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0404") 

quarterly_0404$ageband <- ageband_data_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$age_gender <- age_gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$prescribing_in_children <- child_adult_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0404")

quarterly_0404$monthly_chem_substance <- chem_sub_extract_monthly %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0404")

# 0411 Drugs for dementia workbook - quarterly

quarterly_0411 <- list()

quarterly_0411$patient_id <- capture_rate_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0411") 

quarterly_0411$national_total <- national_extract_quarter %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$national_chem_substance <- chem_sub_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select( `Financial Year`,
                 `Financial Quarter`,
                 `BNF Section Name`,
                 `BNF Section Code`,
                 `BNF Chemical Substance Name`,
                 `BNF Chemical Substance Code`,
                 `Identified Patient Flag`,
                 `Total Identified Patients` = `sdc_Total Identified Patients`,
                 `Total Items` = `sdc_Total Items`,
                 `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$icb <-  icb_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select( `Financial Year`,
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
                 `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$gender <- gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0411") 

quarterly_0411$ageband <- ageband_data_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$age_gender <- age_gender_extract_quarter %>%
  apply_sdc(rounding = F) %>%
  dplyr::select(`Financial Year`,
                `Financial Quarter`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients` = `sdc_Total Identified Patients`,
                `Total Items` = `sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0411")

quarterly_0411$monthly_chem_substance <- chem_sub_extract_monthly %>%
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
    `Total Net Ingredient Cost (GBP)`) %>%
  dplyr::filter(`BNF Section Code` == "0411")

#4. chart data

#chart data for use in markdown

#table 1 patient ID rates
table_1_data <- capture_rate_extract_year |>
  dplyr::select(`BNF Section Name`,
                `BNF Section Code`,
                `2018/2019`,
                `2019/2020`,
                `2020/2021`,
                `2021/2022`,
                `2022/2023`) |>
  dplyr::mutate(across(where(is.numeric), round, 1))
  
table_1 <- table_1_data |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))

#figure 1 annual antidepressant items
figure_1_data <- annual_0403$national_paragraph |>
  dplyr::group_by(`Financial Year`,
                  `BNF Section Name`,
                  `BNF Section Code`,
                  `BNF Paragraph Name`,
                  `BNF Paragraph Code`)|>
  dplyr::summarise(`Total Items` = sum(`Total Items`)) |>
  bind_rows(
    annual_0403$national_total |>
      dplyr::group_by(`Financial Year`,
                      `BNF Section Name`,
                      `BNF Section Code`,
                      `BNF Paragraph Name` = "Total") |> 
      dplyr::summarise(`Total Items` = sum(`Total Items`), .groups = "drop")
  ) |> 
  mutate(`Total Items` = signif(`Total Items`,3))

figure_1 <- figure_1_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Year`,
    y = `Total Items`,
    group = `BNF Paragraph Name`,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of prescribed items",
    dlOn = F,
    title = ""
  ) |> 
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)  

#figure_2 annual antidepressant patients
figure_2_data <- annual_0403$national_paragraph |>
  dplyr::filter(`Identified Patient Flag` != "N") |>
  bind_rows(
    annual_0403$national_total %>%
      dplyr::group_by(
        `Financial Year`,
        `BNF Section Name`,
        `BNF Section Code`,
        `BNF Paragraph Name` = "Total"
      ) |>
      summarise(
        `Total Identified Patients` = sum(`Total Identified Patients`),
        .groups = "drop"
      )
  ) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3)) 
  
figure_2 <- figure_2_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Year`,
    y = `Total Identified Patients`,
    group = `BNF Paragraph Name`,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of identified patients",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

#figure 3 quarterly antidepressant items and patients
figure_3_data <- quarterly_0403$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
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
  dplyr::arrange(desc(measure))

figure_3 <- figure_3_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 4 annual antidepressant age and gender
figure_4_data <- age_gender_extract_year |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403") |>
    dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
    dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3))
  
figure_4 <- figure_4_data |>
    age_gender_chart()
  
#figure 5 annual antidepressant IMD
figure_5_data <- imd_extract_year |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403") |> 
  filter(`Financial Year` == max(`Financial Year`),
         `IMD Quintile` != "Unknown")
  
figure_5 <- figure_5_data |>
  nhsbsaVis::basic_chart_hc(
    x = `IMD Quintile`,
    y = `Total Identified Patients`,
    type = "column",
    xLab = "IMD Quintile",
    yLab = "Number of identified patients",
    title = ""
    )
  
#figure 6 annual antidepressant ICB
figure_6_data <- icb_extract_year |>
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
                 `Total Identified Patients`,
                 `Total Items`,
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403") |>
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::group_by(`Financial Year`,
                  `ICB Name`,
                  `ICB Code`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Total Identified Patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  left_join(icb_pop,
            by = c("ICB Code" = "ICB_CODE")) |>
  mutate(PER_1000 = round(`Total Identified Patients` / POP * 1000, 1)) |>
  arrange(desc(PER_1000)) |>
  filter(`ICB Name` != "UNKNOWN ICB")


figure_6 <- figure_6_data |>
  nhsbsaVis::basic_chart_hc(
    x = `ICB Code`,
    y = PER_1000,
    type = "column",
    xLab = "ICB code",
    yLab = "Patients per 1,000 population",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(
    enabled = T,
    useHTML = TRUE,
    formatter = JS(
      "function(){
                            var result = this.point.ICB_NAME + '<br><b>Patients per 1,000 population:</b> ' + this.point.PER_1000.toFixed(0)
                            return result
             }"
    )
  ) |>
  hc_yAxis(labels = list(enabled = T))

#table_2 annual antidepressant adult child
table_2_data <- child_adult_extract_year |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0403") |>
  dplyr::filter(`Age Band` != "Unknown") |>
  dplyr::group_by(`Financial Year`,
                  `BNF Section Name`,
                  `BNF Section Code`,
                  `Age Band`) |>
  dplyr::summarise(`Total Identified Patients` = formatC(signif(sum(`Total Identified Patients`)/1000000,
                                                                digits = 3),
                                                         digits = 3, format = "fg", flag = "#")) |> 
  dplyr::ungroup() |>
  dplyr::select(`Financial Year`,
                `Age Band`,
                `Total Identified Patients`) |>
  tidyr::pivot_wider(names_from = `Financial Year`,
                     values_from = `Total Identified Patients`)

  
table_2 <-  table_2_data |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0),
                                 list(className = "dt-right", targets = 1:8)
                               )))

#figure 7 annual hypnotics anxiolytics items 
figure_7_data <- annual_0401$national_paragraph |>
  dplyr::group_by(`Financial Year`,
                  `BNF Section Name`,
                  `BNF Section Code`,
                  `BNF Paragraph Name`,
                  `BNF Paragraph Code`) |>
  dplyr::summarise(`Total Items` = sum(`Total Items`)) |>
  bind_rows(
    annual_0401$national_total |>
      dplyr::group_by(`Financial Year`,
                      `BNF Section Name`,
                      `BNF Section Code`,
                      `BNF Paragraph Name` = "Total") |>
      summarise(`Total Items` = sum(`Total Items`), .groups = "drop")
  ) |> 
  mutate(`Total Items` = signif(`Total Items`,3)) 

figure_7 <- figure_7_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Year`,
    y = `Total Items`,
    group = `BNF Paragraph Name`,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of prescribed items",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

#figure 8 annual hypnotics anxiolytics patients
figure_8_data <- annual_0401$national_paragraph |>
  dplyr::filter(`Identified Patient Flag` != "N") |>
  bind_rows(
    annual_0401$national_total |>
      dplyr::group_by(
        `Financial Year`,
        `BNF Section Name`,
        `BNF Section Code`,
        `BNF Paragraph Name` = "Total"
      ) |>
      summarise(
        `Total Identified Patients` = sum(`Total Identified Patients`),
        .groups = "drop"
      )
  ) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3))

figure_8 <- figure_8_data |>
  nhsbsaVis::group_chart_hc(
  x = `Financial Year`,
  y = `Total Identified Patients`,
  group = `BNF Paragraph Name`,
  type = "line",
  xLab = "Financial year",
  yLab = "Number of identified patients",
  dlOn = F,
  title = ""
) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

#figure 9 quarterly hypnotics anxiolytics items and patients
figure_9_data <- quarterly_0401$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
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
  dplyr::arrange(desc(measure))
  
figure_9 <- figure_9_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 10 annual hypnotics anxiolytics age gender
figure_10_data <- age_gender_extract_year |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401") |>
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3))


figure_10 <- figure_10_data |>
  age_gender_chart()

#figure 11 annual hypnotics anxiolytics IMD
figure_11_data <- imd_extract_year |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401") |> 
  filter(`Financial Year` == max(`Financial Year`),
         `IMD Quintile` != "Unknown")

figure_11 <- figure_11_data |>
  nhsbsaVis::basic_chart_hc(
    x = `IMD Quintile`,
    y = `Total Identified Patients`,
    type = "column",
    xLab = "IMD Quintile",
    yLab = "Number of identified patients",
    title = ""
  )

#figure 12 annual hypnotics anxiolytics ICB
figure_12_data <- icb_extract_year |>
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
                 `Total Identified Patients`,
                 `Total Items`,
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0401") |>
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::group_by(`Financial Year`,
                  `ICB Name`,
                  `ICB Code`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Total Identified Patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  left_join(icb_pop,
            by = c("ICB Code" = "ICB_CODE")) |>
  mutate(PER_1000 = round(`Total Identified Patients` / POP * 1000, 1)) |>
  arrange(desc(PER_1000)) |>
  filter(`ICB Name` != "UNKNOWN ICB")

figure_12 <- figure_12_data |>
  nhsbsaVis::basic_chart_hc(
    x = `ICB Code`,
    y = PER_1000,
    type = "column",
    xLab = "ICB code",
    yLab = "Patients per 1,000 population",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(
    enabled = T,
    useHTML = TRUE,
    formatter = JS(
      "function(){
                            var result = this.point.ICB_NAME + '<br><b>Patients per 1,000 population:</b> ' + this.point.PER_1000.toFixed(0)
                            return result
             }"
    )
  ) |>
  hc_yAxis(labels = list(enabled = T))
  
#figure 13 annual antipsychotics items
figure_13_data <- annual_0402$national_paragraph |>
  dplyr::group_by(`Financial Year`,
                  `BNF Section Name`,
                  `BNF Section Code`,
                  `BNF Paragraph Name`,
                  `BNF Paragraph Code`) |>
  dplyr::summarise(`Total Items` = sum(`Total Items`)) |>
  bind_rows(
    annual_0402$national_total |>
      dplyr::group_by(`Financial Year`,
                      `BNF Section Name`,
                      `BNF Section Code`,
                      `BNF Paragraph Name` = "Total") |>
      summarise(`Total Items` = sum(`Total Items`), .groups = "drop")
  ) |> 
  mutate(`Total Items` = signif(`Total Items`,3)) 

figure_13 <- figure_13_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Year`,
    y = `Total Items`,
    group = `BNF Paragraph Name`,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of prescribed items",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

#figure 14 annual antipsychotics patients
figure_14_data <- annual_0402$national_paragraph |>
  dplyr::filter(`Identified Patient Flag` != "N") |>
  bind_rows(
    annual_0402$national_total |>
      dplyr::group_by(
        `Financial Year`,
        `BNF Section Name`,
        `BNF Section Code`,
        `BNF Paragraph Name` = "Total"
      ) |>
      summarise(
        `Total Identified Patients` = sum(`Total Identified Patients`),
        .groups = "drop"
      )
  ) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3))

figure_14 <- figure_14_data |>
  nhsbsaVis::group_chart_hc(
  x = `Financial Year`,
  y = `Total Identified Patients`,
  group = `BNF Paragraph Name`,
  type = "line",
  xLab = "Financial year",
  yLab = "Number of identified patients",
  dlOn = F,
  title = ""
) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

#figure 15 quarterly antipsychotics items and patients
figure_15_data <- quarterly_0402$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
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
  dplyr::arrange(desc(measure))

figure_15 <- figure_15_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 16 annual antipsychotics age gender
figure_16_data <- age_gender_extract_year |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0402") |>
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3))

figure_16 <- figure_16_data |>
  age_gender_chart()

#figure 17 annual antipsychotics IMD
figure_17_data <- imd_extract_year |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0402") |> 
  filter(`Financial Year` == max(`Financial Year`),
         `IMD Quintile` != "Unknown")

figure_17 <- figure_17_data |>
  nhsbsaVis::basic_chart_hc(
    x = `IMD Quintile`,
    y = `Total Identified Patients`,
    type = "column",
    xLab = "IMD Quintile",
    yLab = "Number of identified patients",
    title = ""
  )

#figure 18 annual antipsychotics ICB
figure_18_data <- icb_extract_year |>
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
                 `Total Identified Patients`,
                 `Total Items`,
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0402") |>
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::group_by(`Financial Year`,
                  `ICB Name`,
                  `ICB Code`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Total Identified Patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  left_join(icb_pop,
            by = c("ICB Code" = "ICB_CODE")) |>
  mutate(PER_1000 = round(`Total Identified Patients` / POP * 1000, 1)) |>
  arrange(desc(PER_1000)) |>
  filter(`ICB Name` != "UNKNOWN ICB")


figure_18 <- figure_18_data |>
  nhsbsaVis::basic_chart_hc(
    x = `ICB Code`,
    y = PER_1000,
    type = "column",
    xLab = "ICB code",
    yLab = "Patients per 1,000 population",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(
    enabled = T,
    useHTML = TRUE,
    formatter = JS(
      "function(){
                            var result = this.point.ICB_NAME + '<br><b>Patients per 1,000 population:</b> ' + this.point.PER_1000.toFixed(0)
                            return result
             }"
    )
  ) |>
  hc_yAxis(labels = list(enabled = T))

#figure 19 annual CNS ADHD items
figure_19_data <- annual_0404$national_total |>
  dplyr::group_by(`Financial Year`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(`Total Items` = sum(`Total Items`)) |>
  mutate(`Total Items` = signif(`Total Items`,3))
  
figure_19 <- figure_19_data |>
  nhsbsaVis::group_chart_hc(
  x = `Financial Year`,
  y = `Total Items`,
  group = `BNF Section Name`,
  type = "line",
  xLab = "Financial year",
  yLab = "Number of prescribed items",
  dlOn = F,
  title = ""
  ) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

#figure 20 annual CNS ADHD patients
figure_20_data <- annual_0404$national_total |>
  dplyr::filter(`Identified Patient Flag` != "N") |>
  dplyr::group_by(`Financial Year`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Total Identified Patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3))

figure_20 <- figure_20_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Year`,
    y = `Total Identified Patients`,
    group = `BNF Section Name`,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of identified patients",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

#figure 21 quarterly CNS ADHD items and patients
figure_21_data <- quarterly_0404$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
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
  dplyr::arrange(desc(measure))

figure_21 <- figure_21_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 22 annual CNS ADHD age gender
figure_22_data <- age_gender_extract_year |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404") |>
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3))

figure_22 <- figure_22_data |>
  age_gender_chart()

#figure 23 annual CNS ADHD IMD
figure_23_data <- imd_extract_year |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404") |> 
  filter(`Financial Year` == max(`Financial Year`),
         `IMD Quintile` != "Unknown")

figure_23 <- figure_23_data |>
  nhsbsaVis::basic_chart_hc(
    x = `IMD Quintile`,
    y = `Total Identified Patients`,
    type = "column",
    xLab = "IMD Quintile",
    yLab = "Number of identified patients",
    title = ""
  )

#figure 24 annual CNS ADHD ICB
figure_24_data <- icb_extract_year |>
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
                 `Total Identified Patients`,
                 `Total Items`,
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404") |>
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::group_by(`Financial Year`,
                  `ICB Name`,
                  `ICB Code`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Total Identified Patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  left_join(icb_pop,
            by = c("ICB Code" = "ICB_CODE")) |>
  mutate(PER_1000 = round(`Total Identified Patients` / POP * 1000, 1)) |>
  arrange(desc(PER_1000)) |>
  filter(`ICB Name` != "UNKNOWN ICB")

figure_24 <- figure_24_data |>
  nhsbsaVis::basic_chart_hc(
    x = `ICB Code`,
    y = PER_1000,
    type = "column",
    xLab = "ICB code",
    yLab = "Patients per 1,000 population",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(
    enabled = T,
    useHTML = TRUE,
    formatter = JS(
      "function(){
                            var result = this.point.ICB_NAME + '<br><b>Patients per 1,000 population:</b> ' + this.point.PER_1000.toFixed(0)
                            return result
             }"
    )
  ) |>
  hc_yAxis(labels = list(enabled = T))

#figure 25 annual CNS ADHD adult child
figure_25_data <- child_adult_extract_year |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0404") |> 
  dplyr::filter(`Age Band` != "Unknown") |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `Age Band`,
                `Total Identified Patients`) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`,3)) 

figure_25 <- figure_25_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Year`,
    y = `Total Identified Patients`,
    group = `Age Band`,
    type = "column",
    xLab = "Financial year",
    yLab = "Number of identified patients",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

#figure 26 annual dementia items
figure_26_data <- chem_sub_extract_year %>%
  dplyr::select( `Financial Year`,
                 `BNF Section Name`,
                 `BNF Section Code`,
                 `BNF Chemical Substance Name`,
                 `BNF Chemical Substance Code`,
                 `Identified Patient Flag`,
                 `Total Identified Patients`,
                 `Total Items`,
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411") |>
  dplyr::group_by(`Financial Year`,
                  `BNF Section Name`,
                  `BNF Section Code`,
                  `BNF Chemical Substance Name`,
                  `BNF Chemical Substance Code`) |>
  dplyr::summarise(`Total Items` = sum(`Total Items`)) |>
  bind_rows(
    annual_0411$national_chem_substance |>
      dplyr::group_by(`Financial Year`,
                      `BNF Section Name`,
                      `BNF Section Code`,
                      `BNF Chemical Substance Name` = "Total") |> 
      summarise(`Total Items` = sum(`Total Items`), .groups = "drop")
  ) |> 
  dplyr::mutate(`Total Items` = signif(`Total Items`,3))

figure_26 <- figure_26_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Year`,
    y = `Total Items`,
    group = `BNF Chemical Substance Name`,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of prescribed items",
    dlOn = F,
    title = ""
  ) |> 
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)


#figure 27 annual dementia patients
figure_27_data <- chem_sub_extract_year |>
  dplyr::select( `Financial Year`,
                 `BNF Section Name`,
                 `BNF Section Code`,
                 `BNF Chemical Substance Name`,
                 `BNF Chemical Substance Code`,
                 `Identified Patient Flag`,
                 `Total Identified Patients`,
                 `Total Items`,
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411") |>
  dplyr::filter(`Identified Patient Flag` != "N") |>
  bind_rows(
    annual_0411$national_total |>
      dplyr::group_by(
        `Financial Year`,
        `BNF Section Name`,
        `BNF Section Code`,
        `BNF Chemical Substance Name` = "Total"
      ) |>
      summarise(
        `Total Identified Patients` = sum(`Total Identified Patients`),
        .groups = "drop"
      )
  ) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3))

figure_27 <- figure_27_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Year`,
    y = `Total Identified Patients`,
    group = `BNF Chemical Substance Name`,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of identified patients",
    dlOn = F,
    title = ""
  ) |>
  hc_tooltip(enabled = T,
             shared = T,
             sort = T)

#figure 28 quarterly dementia items and patients
figure_28_data <- quarterly_0411$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
  dplyr::group_by(`Financial Year`, `Financial Quarter`, `BNF Section Name`, `BNF Section Code`) |> 
  dplyr::summarise(`Prescribed items` = sum(`Total Items`),
                   `Identified patients` = sum(`Total Identified Patients`),
                   .groups = "drop") |>
  tidyr::pivot_longer(cols = c(`Identified patients`,`Prescribed items`),
                      names_to = "measure",
                      values_to = "value") |>
  dplyr::mutate(value = signif(value, 3)) |> 
  dplyr::arrange(desc(measure))  

figure_28 <- figure_28_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 29 annual dementia age band
figure_29_data <- age_gender_extract_year |>
  dplyr::select(`Financial Year`,
                `BNF Section Name`,
                `BNF Section Code`,
                `Age Band`,
                `Patient Gender`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "04011") |>
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3)) |> 
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::mutate(`Age Band` = case_when(
    `Age Band` %in% c("90+") ~ "90+",
    `Age Band` %in% c("85-89") ~ "85-89",
    `Age Band` %in% c("80-84") ~ "80-84",
    `Age Band` %in% c("75-79") ~ "75-79",
    `Age Band` %in% c("70-74") ~ "70-74",
    `Age Band` %in% c("65-69") ~ "65-69",
    `Age Band` %in% c("60-64") ~ "60-64",
    `Age Band` %in% c("55-59") ~ "55-59",
    `Age Band` %in% c("50-54") ~ "50-54",
    `Age Band` %in% c("00-04", "10-14", "15-19", "20-24",
                      "25-29", "30-34", "35-39", "40-44", 
                      "45-49") ~ "Under 50",
    TRUE ~ "Unknown")) |>
  dplyr::group_by(`Financial Year`,
                  `BNF Section Name`,
                  `Age Band`,
                  `Patient Gender`) |>
  dplyr::summarise(`Total Identified Patients` = sum(`Total Identified Patients`)) |>
  dplyr::arrange(`Age Band` == "90+",
                 `Age Band` == "85-89",
                 `Age Band` == "80-84",
                 `Age Band` == "75-79",
                 `Age Band` == "70-74",
                 `Age Band` == "65-69",
                 `Age Band` == "60-64",
                 `Age Band` == "55-59",
                 `Age Band` == "50-54",
                 `Age Band` == "Under 50") |>
  dplyr::mutate(`Total Identified Patients` = signif(`Total Identified Patients`, 3)) 

figure_29 <- figure_29_data |>
  age_gender_chart_no_fill()

#figure 30 annual dementia IMD
figure_30_data <- imd_extract_year |>
  dplyr::select(
    `Financial Year`,
    `BNF Section Name`,
    `BNF Section Code`,
    `IMD Quintile`,
    `Total Identified Patients`,
    `Total Items`,
    `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411") |> 
  filter(`Financial Year` == max(`Financial Year`),
         `IMD Quintile` != "Unknown")

figure_30 <- figure_30_data |>
  nhsbsaVis::basic_chart_hc(
    x = `IMD Quintile`,
    y = `Total Identified Patients`,
    type = "column",
    xLab = "IMD Quintile",
    yLab = "Number of identified patients",
    title = "" 
  )

#figure 31 annual dementia ICB
figure_31_data <- icb_extract_year |>
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
                 `Total Identified Patients`,
                 `Total Items`,
                 `Total Net Ingredient Cost (GBP)`) |>
  dplyr::filter(`BNF Section Code` == "0411") |>
  dplyr::filter(`Financial Year` == max(`Financial Year`)) |>
  dplyr::group_by(`Financial Year`,
                  `ICB Name`,
                  `ICB Code`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Total Identified Patients` = sum(`Total Identified Patients`),
    .groups = "drop"
  ) |>
  left_join(icb_pop,
            by = c("ICB Code" = "ICB_CODE")) |>
  mutate(PER_1000 = round(`Total Identified Patients` / POP * 1000, 1)) |>
  arrange(desc(PER_1000)) |>
  filter(`ICB Name` != "UNKNOWN ICB")


figure_31 <- figure_31_data |>
  nhsbsaVis::basic_chart_hc(
    x = `ICB Code`,
    y = PER_1000,
    type = "column",
    xLab = "ICB code",
    yLab = "Patients per 1,000 population",
    dlOn = F,
    title = "") %>% 
  hc_tooltip(enabled = T,
             useHTML = TRUE,
             formatter = JS("function(){
                            var result = this.point.ICB_NAME + '<br><b>Patients per 1,000 population:</b> ' + this.point.PER_1000.toFixed(0)
                            return result
             }")) %>%
  hc_yAxis(
    labels = list(
      enabled = T
    )
  )

#figure 32 antidepressants model data
figure_32_data <- predictions_0403 |>
  dplyr::filter(YEAR_MONTH > 202002)

figure_32 <- figure_32_data |>
  covid_chart_hc(title = "")

#figure 33 hypnotics anxiolytics model data
figure_33_data <- predictions_0401 |>
  dplyr::filter(YEAR_MONTH > 202002)

figure_33 <- figure_33_data |>
  covid_chart_hc(title = "")

#figure 34 antipsychotics model data
figure_34_data <- predictions_0402 |>
  dplyr::filter(YEAR_MONTH > 202002)

figure_34 <- figure_34_data |>
  covid_chart_hc(title = "")

#figure 35 CNS ADHD model data
figure_35_data <- predictions_0404 |>
  dplyr::filter(YEAR_MONTH > 202002)

figure_35 <- figure_35_data |>
  covid_chart_hc(title = "")

#figure 36 dementia model data
figure_36_data <- predictions_0411 |>
  filter(YEAR_MONTH > 202002)

figure_36 <- figure_36_data |>
  covid_chart_hc(title = "")

#5. analysis

#6. covid model

#data manipulation to get 20 year agebands and month columns needed for use in model

df20 <- ageband_manip_20yr(age_gender_extract_month)

#create linear model for each section and apply to 20 year ageband data to get output
#if error thrown by group_mc_cv() function try rerunning covid_lm() function again

model_0401 <- covid_lm(df20,
                       section_code = "0401")
model_0402 <- covid_lm(df20,
                       section_code = "0402")
model_0403 <- covid_lm(df20,
                       section_code = "0403")
model_0404 <- covid_lm(df20,
                       section_code = "0404")
model_0411 <- covid_lm(df20,
                       section_code = "0411")

#get predictions for number of items by BNF section per month

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
covid_model_predictions <- rbind(predictions_0401,
                                 predictions_0402,
                                 predictions_0403,
                                 predictions_0404,
                                 predictions_0411)

# update month in file name for new publications
fwrite(covid_model_predictions, "Y:/Official Stats/MUMH/Covid model tables/Mar23.csv")


#7. tables

#8. render markdown

rmarkdown::render("mumh_narrative_2223_v001.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/mumh_annual_2223_v001.html")
rmarkdown::render("mumh_narrative_2223.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/mumh_annual_2223_v001.docx")
rmarkdown::render("background.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/background.html")
rmarkdown::render("background.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/background.docx")