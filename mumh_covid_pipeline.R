#pipeline.R
#this script provides the code to run the reproducible analytical pipeline
#and produce the Medicines Used in Mental Health (MUMH) publication

#clear environment
rm(list = ls())

#source functions - commented out while I check if required/ add functions to folder
#this is only a temporary step until all functions are built into packages
source("./functions/functions.R")
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

#2. data import
con <- nhsbsaR::con_nhsbsa(database = "DWCP")

#dispensing days data
dispensing_days_data <- nhsbsaUtils::dispensing_days(2023)

#monthly data extracts


age_gender_extract_month <- age_gender_extract_period(con = con,
                                                      period_type = "month")

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
