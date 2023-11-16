#pipeline_base.R
#this script provides code to run the reproducible analytical pipeline
#and produce the Medicines Used in Mental Health (MUMH) publication 
#it must be used alongside the pipelines for each BNF section

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

#1. Install required packages
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

#2. Data import for all 5 sections [incomplete]

con <- con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP",
  username = rstudioapi::showPrompt(title = "Username", message = "Username"),
  password = rstudioapi::askForPassword()
)

#save data if new data found 
#append to previous stored data

#load reference data

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

#3. Data extracts

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

#get 20 year agebands and month columns needed for use in model

df20 <- ageband_manip_20yr(age_gender_extract_month)


#4. Run individual pipelines 
#pipelines for each section can be found in the MUMH git repo 
#https://github.com/nhsbsa-data-analytics/medicines-used-in-mental-health

#5. QR 

#store model predictions as excel workbook for QR
covid_model_predictions <- rbind(predictions_0401,
                                 predictions_0402,
                                 predictions_0403,
                                 predictions_0404,
                                 predictions_0411)

# update month in file name for new publications
fwrite(covid_model_predictions, "Y:/Official Stats/MUMH/Covid model tables/Jun23.csv")

#6. render markdown outputs as html for web publishing and word doc for QR
#render narratives within individual section pipelines during step 4 above

#render files common to all sections
#render background
rmarkdown::render("background.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/background.html")
rmarkdown::render("background.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/background.docx")
#render user engagement
rmarkdown::render("background.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/background.html")
rmarkdown::render("background.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/background.docx")
#render pre-release access list
rmarkdown::render("PRA_list_ytd_jun23.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/PRA_list_ytd_jun23.html")