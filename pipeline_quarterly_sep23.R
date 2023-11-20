#pipeline_quarterly.R
#this script provides the code to run the reproducible analytical pipeline
#and produce the Medicines Used in Mental Health (MUMH) quarterly publication

# clear environment
rm(list = ls())

# source functions
# select all .R files in functions sub-folder
function_files <- list.files(path = "functions", pattern = "\\.R$")

# loop over function_files to source all files in functions sub-folder
for (file in function_files) {
  source(file.path("functions", file))
}

#1. Setup and package installation

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
devtools::install_github("nhsbsa-data-analytics/nhsbsaUtils",
                         auth_token = Sys.getenv("GITHUB_PAT"), force = TRUE)

library(nhsbsaUtils)

# install required packages
# double check required packages once full pipeline built eg. if maps used
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
              "svDialogs",
              "tcltk",
              "tidyr",
              "tidyverse",
              "vroom",
              "yaml")

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

#2. Data import

con <- nhsbsaR::con_nhsbsa(dsn = "FBS_8192k",
                           driver = "Oracle in OraClient19Home1",
                           "DWCP")

schema <-
  as.character(svDialogs::dlgInput("Enter schema name: ")$res)

# quarterly data extracts

capture_rate_extract_quarter <- capture_rate_extract_period(con = con,
                                                            schema = schema,
                                                            table = config$sql_table_name,
                                                            period_type = "quarter")
national_extract_quarter <- national_extract_period(con = con,
                                                    schema = schema,
                                                    table = config$sql_table_name,
                                                    period_type = "quarter")
paragraph_extract_quarter <- paragraph_extract_period(con = con,
                                                      schema = schema,
                                                      table = config$sql_table_name,
                                                      period_type = "quarter")
chem_sub_extract_quarter <- chem_sub_extract_period(con = con,
                                                    schema = schema,
                                                    table = config$sql_table_name,
                                                    period_type = "quarter")
icb_extract_quarter <- icb_extract_period(con = con,
                                          schema = schema,
                                          table = config$sql_table_name,
                                          period_type = "quarter")
ageband_data_quarter <- ageband_extract_period(con = con,
                                               schema = schema,
                                               table = config$sql_table_name,
                                               period_type = "quarter")
gender_extract_quarter <- gender_extract_period(con = con,
                                                schema = schema,
                                                table = config$sql_table_name,
                                                period_type = "quarter")
age_gender_extract_quarter <- age_gender_extract_period(con = con,
                                                        schema = schema,
                                                        table = config$sql_table_name,
                                                        period_type = "quarter")
imd_extract_quarter <- imd_extract_period(con = con,
                                          schema = schema,
                                          table = config$sql_table_name,
                                          period_type = "quarter")
child_adult_extract_quarter <- child_adult_extract(con = con,
                                                   schema = schema,
                                                   table = config$sql_table_name,
                                                   period_type = "quarter")

log_print("Quarterly extracts pulled", hide_notes = TRUE)

# monthly data extracts

national_extract_monthly <- national_extract_period(con = con,
                                                    schema = schema,
                                                    table = config$sql_table_name,
                                                    period_type = "month")
paragraph_extract_monthly <- paragraph_extract_period(con = con,
                                                      schema = schema,
                                                      table = config$sql_table_name,
                                                      period_type = "month")
chem_sub_extract_monthly <- chem_sub_extract_period(con = con,
                                                    schema = schema,
                                                    table = config$sql_table_name,
                                                    period_type = "month")
age_gender_extract_month <- age_gender_extract_period(con = con,
                                                      schema = schema,
                                                      table = config$sql_table_name,
                                                      period_type = "month")

log_print("Monthly extracts pulled", hide_notes = TRUE)

# disconnect from data warehouse once all extracts pulled

DBI::dbDisconnect(con)

#3. Aggregations and analysis

#0401 Hypnotics and anxiolytics workbook - quarterly


