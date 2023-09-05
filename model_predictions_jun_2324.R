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

fwrite(covid_model_predictions_aug, "Y:/Official Stats/MUMH/Covid model tables/Jun23.csv")