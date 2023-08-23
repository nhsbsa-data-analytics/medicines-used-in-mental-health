#pipeline_antidepressants.R
#this script provides code to run the analysis of BNF section 0403 antidepressants
#before running this script, run pipeline_base.R until step 4

#4.1 Data extracts and analysis

# 0403 Antidepressants - annual
#filter main extracts to annual antidepressants only and store as list
#TO DO: configure this as year-to-date figures instead

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

#4.2 Chart data

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

#4.3 modelling

#create linear model and apply to 20 year ageband data to get output
model_0403 <- covid_lm(df20,
                       section_code = "0403")

#get predictions for number of items by BNF section per month
predictions_0403 <- prediction_list(df20,
                                    "0403",
                                    model_0403,
                                    pred_month_list)

#4.4 Tables
#create and format excel tables 
# TO DO: amend for ytd June 2023/24

### 0403 antidepressants - annual

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "National_Population",
  "National_Paragraph",
  "ICB",
  "Gender",
  "Age_Band",
  "Age_Band_and_Gender",
  "IMD",
  "Presc_in_Children"
)

wb <- accessibleTables::create_wb(sheetNames)

#create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "BNF Section Name",
  "BNF Section Code",
  "Identified Patient Flag",
  "Total Identified Patients",
  "Total Items",
  "Total Net Ingredient Cost (GBP)",
  "Financial Year",
  "Mid-Year Population Year",
  "Mid-Year England Population Estimate",
  "Patients per 1000 Population",
  "BNF Paragraph Name",
  "BNF Paragraph Code",
  "ICB Name",
  "ICB Code",
  "BNF Chemical Substance Name",
  "BNF Chemical Substance Code",
  "Patient Gender",
  "Age Band",
  "IMD Quintile"
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
    "The year in which population estimates were taken, required due to the presentation of this data in financial year format.",
    "The population estimate for the corresponding Mid-Year Population Year.",
    "(Total Identified Patients / Mid-Year England Population Estimate) * 1000.",
    "The name given to the British National Formular (BNF) paragraph. This level of grouping of the BNF Therapeutical classification system sits below BNF section.",
    "The unique code used to refer to the British National Formulary (BNF) paragraph.",
    "The name given to the Integrated Care Board (ICB) that a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
    "The unique code used to refer to an Integrated Care Board (ICB).",
    "The name of the main active ingredient in a drug. Appliances do not hold a chemical substance, but instead inherit the corresponding BNF section. Determined by the British National Formulatory (BNF) for drugs, or the NHS BSA for appliances. For example, Amoxicillin.",
    "The unique code used to refer to the British National Formulary (BNF) chemical substance.",
    "The gender of the patient as at the time the prescription was processed. Please see the detailed Background Information and Methodology notice released with this publication for further information.",
    "The age band of the patient as of the 30th September of the corresponding financial year the drug was prescribed.",
    "The IMD quintile of the patient, based on the patient's postcode, where '1' is the 20% of areas with the highest deprivation score in the Index of Multiple Deprivation (IMD) from the English Indices of Deprivation 2019, and '5' is the 20% of areas with the lowest IMD deprivation score. The IMD quintile has been recorded as 'Unknown' where the items are attributed to an unidentified patient, or where we have been unable to match the patient postcode to a postcode in the National Statistics Postcode Lookup (NSPL)."
  )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)

## Patient identification

# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Patient_Identification",
  "Medicines Used in Mental Health - England - 2015/16 to 2022/23 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  annual_0403$patient_id,
  42
)

#left align columns A to C
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

#right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("C", "D", "E", "F", "G", "H", "I", "J"),
                              "right",
                              "0.00")

## National Total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  "Medicines Used in Mental Health - England - 2015/16 to 2022/23 - Yearly totals by identified patients",
  c("1. Field definitions can be found on the 'Metadata' tab."),
  annual_0403$national_total,
  14
)

#left align columns A to D
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D"),
                              "left",
                              "")

#right align columns E and F and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("E", "F"),
                              "right",
                              "#,##0")

#right align column G and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("G"),
                              "right",
                              "#,##0.00")

## National Population

accessibleTables::write_sheet(
  wb,
  "National_Population",
  "Medicines Used in Mental Health - England - 2015/16 to 2022/23 - Population totals by financial year",
  c(
    "1. Some cells in this table are empty because ONS population estimates for 2022/2023 were not available prior to publication.",
    "2. ONS population estimates taken from https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates."
  ),
  annual_0403$national_population,
  14
)

#left align columns A to D
accessibleTables::format_data(wb,
                              "National_Population",
                              c("A", "B", "C", "D"),
                              "left",
                              "")

#right align columns E and F and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Population",
                              c("E", "F"),
                              "right",
                              "#,##0")

#right align column G and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Population",
                              c("G"),
                              "right",
                              "#,##0.00")

## National Paragraph

accessibleTables::write_sheet(
  wb,
  "National_Paragraph",
  "Medicines Used in Mental Health - England - 2015/16 to 2021/22 - Yearly totals by BNF paragraph and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear blank."
  ),
  annual_0403$national_paragraph,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("I"),
                              "right",
                              "#,##0.00")

## ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  "Medicines Used in Mental Health - England - 2015/16 to 2022/23 - Yearly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear blank."
  ),
  annual_0403$icb,
  14
)

#left align columns A to L
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J"),
                              "left",
                              "")

#right align columns M and N and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("K", "L", "M"),
                              "right",
                              "#,##0")

#right align column O and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("O"),
                              "right",
                              "#,##0.00")

## Gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  "Medicines Used in Mental Health - England - 2015/16 to 2022/23 - Yearly totals by gender",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as blank.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together."
  ),
  annual_0403$gender,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column H and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("H"),
                              "right",
                              "#,##0.00")

## Age Band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  "Medicines Used in Mental Health - England - 2015/16 to 2022/23 - Yearly totals by age band",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear blank."
  ),
  annual_0403$ageband,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column H and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("H"),
                              "right",
                              "#,##0.00")

## Age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  "Medicines Used in Mental Health - England - 2015/16 to 2022/23 - Yearly totals by age band and gender",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear blank.",
    "3. These totals only include patients where both age and gender are known."
  ),
  annual_0403$age_gender,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

## IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  "Medicines Used in Mental Health - England - 2015/16 to 2022/23 - Yearly totals by IMD",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear blank.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile."
  ),
  annual_0403$imd,
  14
)

#left align columns A to D
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D"),
                              "left",
                              "")

#right align columns E and F and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("E", "F"),
                              "right",
                              "#,##0")

#right align column G and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("G"),
                              "right",
                              "#,##0.00")

## Prescribing in children

write_sheet(
  wb,
  "Presc_in_Children",
  "Medicines Used in Mental Health - England - 2015/16 to 2022/23 - Prescribing in adults and children, age bands 17 and under to 18 and over",
  c("1. Field definitions can be found on the 'Metadata' tab."),
  annual_0403$prescribing_in_children,
  14
)

#left align columns A to D
format_data(wb,
            "Presc_in_Children",
            c("A", "B", "C", "D"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Presc_in_Children",
            c("E", "F"),
            "right",
            "#,##0")

#right align column G and round to 2dp with thousand separator
format_data(wb,
            "Presc_in_Children",
            c("G"),
            "right",
            "#,##0.00")

#create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0403 Antidepressant drugs",
  "England 2015/16 - 2022/23",
  "Publication Date: 6 July 2023",
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: Patient Identification Rates",
    "Table 2: National Total",
    "Table 3: National Population",
    "Table 4: National Paragraph",
    "Table 5: ICB",
    "Table 6: Gender",
    "Table 7: Age Band",
    "Table 8: Age Band and Sex",
    "Table 9: Indices of Deprivation (IMD)",
    "Table 10: Prescribing in Children"
  ),
  c("Metadata", sheetNames)
)


#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0403_2022_23_v001.xlsx",
                       overwrite = TRUE)


#4.5. render markdown outputs as html for web publishing and word doc for QR

#0403 antidepressants narrative

rmarkdown::render("mumh_narrative_ytd_jun23_0403.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/mumh_ytd_jun23_0403.html")
rmarkdown::render("mumh_narrative_ytd_jun23.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/mumh_ytd_jun23.docx")

