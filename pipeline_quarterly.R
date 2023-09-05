#pipeline.R
#this script provides the code to run the reproducible analytical pipeline
#and produce the Medicines Used in Mental Health (MUMH) quarterly publication

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

con <- nhsbsaR::con_nhsbsa(dsn = "FBS_8192k",
                           driver = "Oracle in OraClient19Home1",
                           "DWCP")



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


#0401 Hypnotics and anxiolytics workbook - quarterly

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



#3. covid model

#data manipulation to get 20 year agebands and month columns needed for use in model

#script showing code used for model of item predictions extrapolated from
#pre-covid pandemic prescribing trends

#To be used as part of main pipeline 
#make sure table names and dates in underlying functions are updated before 
#running this code

#get dispensing days for up to 2023/24 financial year
dispensing_days_data <- nhsbsaUtils::dispensing_days(2024)

age_gender_extract_month <- age_gender_extract_period(con = con,
                                                      period_type = "month")

df20 <- ageband_manip_20yr(age_gender_extract_month)

#split df20 data into pre-covid months, to build model on pre-pandemic data only
df20_pc <- df20 |>
  dplyr::filter(time_period == "pre_covid")

#build model for each BNF section, using the pre-pandemic items
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

#get predictions for number of items by BNF section per month
#predictions are extrapolations of pre-covid trends, applied to full df20 data
#so will only predict for March 2020 onwards

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
covid_model_predictions_aug <- rbind(predictions_0401,
                                     predictions_0402,
                                     predictions_0403,
                                     predictions_0404,
                                     predictions_0411)


# update month in file name for new publications
fwrite(covid_model_predictions, "Y:/Official Stats/MUMH/Covid model tables/Jun23.csv")


#4. chart data

#chart data for use in markdown

#table 1 patient ID rates by section
#antidepressants 0403
table_1_data_0403 <- capture_rate_extract_quarter |>
  dplyr::select(`BNF Section Name`,
                `BNF Section Code`,
                `2022/2023 Q2`,
                `2022/2023 Q3`,
                `2022/2023 Q4`,
                `2023/2024 Q1`) |>
  dplyr::filter(`BNF Section Code` == "0403") |>
  dplyr::mutate(across(where(is.numeric), round, 1))
  
table_1_0403 <- table_1_data_0403 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))
#Hypnotics and anxiolytics 0401
table_1_data_0401 <- capture_rate_extract_quarter |>
  dplyr::select(`BNF Section Name`,
                `BNF Section Code`,
                `2022/2023 Q2`,
                `2022/2023 Q3`,
                `2022/2023 Q4`,
                `2023/2024 Q1`) |>
  dplyr::filter(`BNF Section Code` == "0401") |>
  dplyr::mutate(across(where(is.numeric), round, 1))

table_1_0401 <- table_1_data_0401 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))
#Drugs used in psychoses and related disorders 0402
table_1_data_0402 <- capture_rate_extract_quarter |>
  dplyr::select(`BNF Section Name`,
                `BNF Section Code`,
                `2022/2023 Q2`,
                `2022/2023 Q3`,
                `2022/2023 Q4`,
                `2023/2024 Q1`) |>
  dplyr::filter(`BNF Section Code` == "0402") |>
  dplyr::mutate(across(where(is.numeric), round, 1))

table_1_0402 <- table_1_data_0402 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))
#CNS stimulants and drugs used for ADHD 0404
table_1_data_0404 <- capture_rate_extract_quarter |>
  dplyr::select(`BNF Section Name`,
                `BNF Section Code`,
                `2022/2023 Q2`,
                `2022/2023 Q3`,
                `2022/2023 Q4`,
                `2023/2024 Q1`) |>
  dplyr::filter(`BNF Section Code` == "0404") |>
  dplyr::mutate(across(where(is.numeric), round, 1))

table_1_0404 <- table_1_data_0404 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))
#dementia 0411
table_1_data_0411 <- capture_rate_extract_quarter |>
  dplyr::select(`BNF Section Name`,
                `BNF Section Code`,
                `2022/2023 Q2`,
                `2022/2023 Q3`,
                `2022/2023 Q4`,
                `2023/2024 Q1`) |>
  dplyr::filter(`BNF Section Code` == "0411") |>
  dplyr::mutate(across(where(is.numeric), round, 1))

table_1_0411 <- table_1_data_0411 |>
  DT::datatable(rownames = FALSE,
                options = list(dom = "t",
                               columnDefs = list(
                                 list(orderable = FALSE,
                                      targets = "_all"),
                                 list(className = "dt-left", targets = 0:1),
                                 list(className = "dt-right", targets = 2:5)
                               )))

#figure 1 quarterly antidepressant items and patients
figure_1_data <- quarterly_0403$national_total |>
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

figure_1 <- figure_1_data |>
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
#figure 2 quarterly antidepressant cost
figure_2_data <- quarterly_0403$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure))

figure_2 <- figure_2_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 3 monthly antidepressant items and patients
figure_3_data <- quarterly_0403$monthly_section |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
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
  dplyr::arrange(desc(measure))

figure_3 <- figure_3_data |>
  nhsbsaVis::group_chart_hc(
    x = MONTH_START,
    y = value,
    group = measure,
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
  hc_xAxis(type = "datetime",
           dateTimeLabelFormats = list(month = "%b %y"),
           title = list(text = "Month")) 

#figure 4 quarterly hypnotics and anxiolytics items and patients
figure_4_data <- quarterly_0401$national_total |>
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

figure_4 <- figure_4_data |>
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
#figure 5 quarterly hypnotics and anxiolytics cost
figure_5_data <- quarterly_0401$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure))

figure_5 <- figure_5_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 6 monthly hypnotics and anxiolytics items and patients
figure_6_data <- quarterly_0401$monthly_section |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
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
  dplyr::arrange(desc(measure))

figure_6 <- figure_6_data |>
  nhsbsaVis::group_chart_hc(
    x = MONTH_START,
    y = value,
    group = measure,
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
  hc_xAxis(type = "datetime",
           dateTimeLabelFormats = list(month = "%b %y"),
           title = list(text = "Month")) 

#figure 7 quarterly antipsychotic items and patients
figure_7_data <- quarterly_0402$national_total |>
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

figure_7 <- figure_7_data |>
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
#figure 8 quarterly antipsychotic cost
figure_8_data <- quarterly_0402$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure))

figure_8 <- figure_8_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 9 monthly antipsychotic items and patients
figure_9_data <- quarterly_0402$monthly_section |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
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
  dplyr::arrange(desc(measure))

figure_9 <- figure_9_data |>
  nhsbsaVis::group_chart_hc(
    x = MONTH_START,
    y = value,
    group = measure,
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
  hc_xAxis(type = "datetime",
           dateTimeLabelFormats = list(month = "%b %y"),
           title = list(text = "Month")) 

#figure 10 quarterly CNS ADHD items and patients
figure_10_data <- quarterly_0404$national_total |>
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

figure_10 <- figure_10_data |>
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
#figure 11 quarterly CNS ADHD cost
figure_11_data <- quarterly_0404$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure))

figure_11 <- figure_11_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 12 monthly CNS ADHD items and patients
figure_12_data <- quarterly_0404$monthly_section |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
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
  dplyr::arrange(desc(measure))

figure_12 <- figure_12_data |>
  nhsbsaVis::group_chart_hc(
    x = MONTH_START,
    y = value,
    group = measure,
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
  hc_xAxis(type = "datetime",
           dateTimeLabelFormats = list(month = "%b %y"),
           title = list(text = "Month")) 

#figure 13 quarterly dementia items and patients
figure_13_data <- quarterly_0411$national_total |>
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

figure_13 <- figure_13_data |>
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
#figure 14 quarterly dementia cost
figure_14_data <- quarterly_0411$national_total |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
  dplyr::group_by(`Financial Year`,
                  `Financial Quarter`,
                  `BNF Section Name`,
                  `BNF Section Code`) |>
  dplyr::summarise(
    `Cost (GBP)` = sum(`Total Net Ingredient Cost (GBP)`),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(
    cols = c(`Cost (GBP)`),
    names_to = "measure",
    values_to = "value"
  ) |>
  dplyr::mutate(value = signif(value, 3)) |>
  dplyr::arrange(desc(measure))

figure_2 <- figure_2_data |>
  nhsbsaVis::group_chart_hc(
    x = `Financial Quarter`,
    y = value,
    group = measure,
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
  hc_xAxis(
    labels = list(
      step = 2,
      rotation = -45
    )
  )

#figure 15 monthly dementia items and patients
figure_15_data <- quarterly_0411$monthly_section |>
  #added financial year filter now only using rolling years
  #dplyr::filter(`Financial Year` %!in% c("2015/2016", "2016/2017")) |>
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
  dplyr::arrange(desc(measure))

figure_15 <- figure_15_data |>
  nhsbsaVis::group_chart_hc(
    x = MONTH_START,
    y = value,
    group = measure,
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
  hc_xAxis(type = "datetime",
           dateTimeLabelFormats = list(month = "%b %y"),
           title = list(text = "Month")) 

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


#5. render markdown


rmarkdown::render("mumh-quarterly-narrative-0403.Rmd",
                  output_format = "html_document",
                  output_file = "mumh-quarterly-narrative-0403.html")
rmarkdown::render("mumh-quarterly-narrative-0403",
                  output_format = "word_document",
                  output_file = "mumh-quarterly-narrative-0403.docx")
rmarkdown::render("mumh-quarterly-narrative-0401.Rmd",
                  output_format = "html_document",
                  output_file = "mumh-quarterly-narrative-0401.html")
rmarkdown::render("mumh-quarterly-narrative-0402",
                  output_format = "word_document",
                  output_file = "mumh-quarterly-narrative-0402.docx")
rmarkdown::render("mumh-quarterly-narrative-0404.Rmd",
                  output_format = "html_document",
                  output_file = "mumh-quarterly-narrative-0404.html")
rmarkdown::render("mumh-quarterly-narrative-0411",
                  output_format = "word_document",
                  output_file = "mumh-quarterly-narrative-0411.docx")
rmarkdown::render("mumh-quarterly-narrative-0403covid.Rmd",
                  output_format = "html_document",
                  output_file = "mumh-quarterly-narrative-0403covid.html")
rmarkdown::render("mumh-quarterly-narrative-0403covid.Rmd",
                  output_format = "html_document",
                  output_file = "mumh-quarterly-narrative-0403covid.html")

rmarkdown::render("background.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/background.html")
rmarkdown::render("background.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/background.docx")
