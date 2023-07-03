#code for use in main MUMH pipeline to create excel tables

# 0401 Hypnotics and anxiolytics - quarterly and monthly

sheetNames <- c(
  "Patient_Identification",
  "National_Total",
  "National_Paragraph",
  "ICB",
  "Gender",
  "Age_Band",
  "Age_Band_and_Gender",
  "IMD",
  "Monthly_Section",
  "Monthly_Paragraph"
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
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0401$patient_id,
  42
)

#left align columns A to C
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

#right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
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

## National Total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by identified patients",
  c("1. Field definitions can be found on the 'Metadata' tab."),
  quarterly_0401$national_total,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns E and F and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column G and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


## National Paragraph

accessibleTables::write_sheet(
  wb,
  "National_Paragraph",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by BNF paragraph and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0401$national_paragraph,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("J"),
                              "right",
                              "#,##0.00")

## ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0401$icb,
  14
)

#left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

#right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

#right align column N and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

## Gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together."
  ),
  quarterly_0401$gender,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age Band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0401$ageband,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band and gender",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. These totals only include patients where both age and gender are known."
  ),
  quarterly_0401$age_gender,
  14
)

#left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column J and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

## IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by IMD",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile."
  ),
  quarterly_0401$imd,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column H and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

##  Monthly section data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF section",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
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

##  Monthly paragraph data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Paragraph",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF paragraph",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
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

#create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0401 Hypnotics and anxiolytics",
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
    "Table 10: Monthly Section",
    "Table 11: Monthly Paragraph"
  ),
  c("Metadata", sheetNames)
)

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0401_Mar_23_test.xlsx",
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
  "IMD",
  "Monthly_Section",
  "Monthly_Paragraph"
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
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0402$patient_id,
  42
)

#left align columns A to C
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

#right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
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

## National Total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by identified patients",
  c("1. Field definitions can be found on the 'Metadata' tab."),
  quarterly_0402$national_total,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns E and F and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column G and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


## National Paragraph

accessibleTables::write_sheet(
  wb,
  "National_Paragraph",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by BNF paragraph and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0402$national_paragraph,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("J"),
                              "right",
                              "#,##0.00")

## ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0402$icb,
  14
)

#left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

#right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

#right align column N and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

## Gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together."
  ),
  quarterly_0402$gender,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age Band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0402$ageband,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band and gender",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. These totals only include patients where both age and gender are known."
  ),
  quarterly_0402$age_gender,
  14
)

#left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column J and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

## IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by IMD",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile."
  ),
  quarterly_0402$imd,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column H and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

##  Monthly section data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF section",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
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

##  Monthly paragraph data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Paragraph",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF paragraph",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
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

#create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0402 Drugs used in psychoses and related disorders",
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
    "Table 10: Monthly Section",
    "Table 11: Monthly Paragraph"
  ),
  c("Metadata", sheetNames)
)

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0402_Mar_23_test.xlsx",
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
  "IMD",
  "Presc_in_Children",
  "Monthly_Section",
  "Monthly_Paragraph"
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
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0403$patient_id,
  42
)

#left align columns A to C
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

#right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
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

## National Total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by identified patients",
  c("1. Field definitions can be found on the 'Metadata' tab."),
  quarterly_0403$national_total,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns E and F and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column G and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


## National Paragraph

accessibleTables::write_sheet(
  wb,
  "National_Paragraph",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by BNF paragraph and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0403$national_paragraph,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Paragraph",
                              c("J"),
                              "right",
                              "#,##0.00")

## ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0403$icb,
  14
)

#left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

#right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

#right align column N and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

## Gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together."
  ),
  quarterly_0403$gender,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age Band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0403$ageband,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band and gender",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. These totals only include patients where both age and gender are known."
  ),
  quarterly_0403$age_gender,
  14
)

#left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column J and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

## IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by IMD",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile."
  ),
  quarterly_0403$imd,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column H and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

## Prescribing in children

write_sheet(
  wb,
  "Presc_in_Children",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Prescribing in adults and children, age bands 17 and under to 18 and over",
  c("1. Field definitions can be found on the 'Metadata' tab."),
  quarterly_0403$prescribing_in_children,
  14
)

#left align columns A to e
format_data(wb,
            "Presc_in_Children",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

#right align columns F and G and round to whole numbers with thousand separator
format_data(wb,
            "Presc_in_Children",
            c("F", "G"),
            "right",
            "#,##0")

#right align column H and round to 2dp with thousand separator
format_data(wb,
            "Presc_in_Children",
            c("H"),
            "right",
            "#,##0.00")

##  Monthly section data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF section",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
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

##  Monthly paragraph data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Paragraph",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF paragraph",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
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
    "Table 10: Prescribing in Children",
    "Table 11: Monthly Section",
    "Table 12: Monthly Paragraph"
  ),
  c("Metadata", sheetNames)
)

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0403_Mar_23_test.xlsx",
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
  "IMD",
  "Presc_in_Children",
  "Monthly_Section",
  "Monthly_Chemical_Substance"
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
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0404$patient_id,
  42
)

#left align columns A to C
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

#right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
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

## National Total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by identified patients",
  c("1. Field definitions can be found on the 'Metadata' tab."),
  quarterly_0404$national_total,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns E and F and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column G and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


## National Chemical Substance

accessibleTables::write_sheet(
  wb,
  "National_Chemical_Substance",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by BNF chemical substance and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0404$national_chem_substance,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("J"),
                              "right",
                              "#,##0.00")

## ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0404$icb,
  14
)

#left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

#right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

#right align column N and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

## Gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together."
  ),
  quarterly_0404$gender,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age Band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0404$ageband,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band and gender",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. These totals only include patients where both age and gender are known."
  ),
  quarterly_0404$age_gender,
  14
)

#left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column J and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

## IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by IMD",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile."
  ),
  quarterly_0404$imd,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column H and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

## Prescribing in children

write_sheet(
  wb,
  "Presc_in_Children",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Prescribing in adults and children, age bands 17 and under to 18 and over",
  c("1. Field definitions can be found on the 'Metadata' tab."),
  quarterly_0404$prescribing_in_children,
  14
)

#left align columns A to e
format_data(wb,
            "Presc_in_Children",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

#right align columns F and G and round to whole numbers with thousand separator
format_data(wb,
            "Presc_in_Children",
            c("F", "G"),
            "right",
            "#,##0")

#right align column H and round to 2dp with thousand separator
format_data(wb,
            "Presc_in_Children",
            c("H"),
            "right",
            "#,##0.00")

##  Monthly section data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF section",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
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

##  Monthly chemical substance data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Chemical_Substance",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF chemical substance",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
  ),
  quarterly_0404$monthly_chem_substance,
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
#create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0404 Central nervous system (CNS) stimulants and drugs used for ADHD",
  "England 2015/16 - 2022/23",
  "Publication Date: 6 July 2023",
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: Patient Identification Rates",
    "Table 2: National Total",
    "Table 3: National Population",
    "Table 4: National Chemical Substance",
    "Table 5: ICB",
    "Table 6: Gender",
    "Table 7: Age Band",
    "Table 8: Age Band and Sex",
    "Table 9: Indices of Deprivation (IMD)",
    "Table 10: Prescribing in Children",
    "Table 11: Monthly Section",
    "Table 12: Monthly Chemical Substance"
  ),
  c("Metadata", sheetNames)
)

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0404_Mar_23_test.xlsx",
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
  "IMD",
  "Monthly_Section",
  "Monthly_Chemical_Substance"
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
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  quarterly_0411$patient_id,
  42
)

#left align columns A to C
accessibleTables::format_data(wb,
                              "Patient_Identification",
                              c("A", "B"),
                              "left",
                              "")

#right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
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

## National Total

accessibleTables::write_sheet(
  wb,
  "National_Total",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by identified patients",
  c("1. Field definitions can be found on the 'Metadata' tab."),
  quarterly_0411$national_total,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "National_Total",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns E and F and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column G and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Total",
                              c("H"),
                              "right",
                              "#,##0.00")


## National Chemical Substance

accessibleTables::write_sheet(
  wb,
  "National_Chemical_Substance",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals split by BNF chemical substance and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0411$national_chem_substance,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "National_Chemical_Substance",
                              c("J"),
                              "right",
                              "#,##0.00")

## ICB

accessibleTables::write_sheet(
  wb,
  "ICB",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0411$icb,
  14
)

#left align columns A to K
accessibleTables::format_data(wb,
                              "ICB",
                              c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"),
                              "left",
                              "")

#right align columns L and M and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("L", "M"),
                              "right",
                              "#,##0")

#right align column N and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "ICB",
                              c("N"),
                              "right",
                              "#,##0.00")

## Gender

accessibleTables::write_sheet(
  wb,
  "Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by ICB",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. It is possible for a patient to be codified with gender 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together."
  ),
  quarterly_0411$gender,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Gender",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Gender",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age Band

accessibleTables::write_sheet(
  wb,
  "Age_Band",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1."
  ),
  quarterly_0411$ageband,
  14
)

#left align columns A to F
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("A", "B", "C", "D", "E", "F"),
                              "left",
                              "")

#right align columns G and H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("G", "H"),
                              "right",
                              "#,##0")

#right align column I and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band",
                              c("I"),
                              "right",
                              "#,##0.00")

## Age and gender

accessibleTables::write_sheet(
  wb,
  "Age_Band_and_Gender",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by age band and gender",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. These totals only include patients where both age and gender are known."
  ),
  quarterly_0411$age_gender,
  14
)

#left align columns A to G
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("A", "B", "C", "D", "E", "F", "G"),
                              "left",
                              "")

#right align columns H and I and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("H", "I"),
                              "right",
                              "#,##0")

#right align column J and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "Age_Band_and_Gender",
                              c("J"),
                              "right",
                              "#,##0.00")

## IMD

accessibleTables::write_sheet(
  wb,
  "IMD",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Quarterly totals by IMD",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. Where a patient's postcode has not been able to to be matched to NSPL or the patient has not been identified the records are reported as 'unknown' IMD quintile."
  ),
  quarterly_0411$imd,
  14
)

#left align columns A to E
accessibleTables::format_data(wb,
                              "IMD",
                              c("A", "B", "C", "D", "E"),
                              "left",
                              "")

#right align columns F and G and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("F", "G"),
                              "right",
                              "#,##0")

#right align column H and round to 2dp with thousand separator
accessibleTables::format_data(wb,
                              "IMD",
                              c("H"),
                              "right",
                              "#,##0.00")

##  Monthly section data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Section",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF section",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
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

##  Monthly chemical substance data
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Monthly_Chemical_Substance",
  "Medicines Used in Mental Health - England - April 2015 to March 2023 - Monthly totals by BNF chemical substance",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear as -1.",
    "3. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
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

#create cover sheet
accessibleTables::makeCoverSheet(
  "Medicines Used in Mental Health - BNF 0411 Drugs for dementia",
  "England 2015/16 - 2022/23",
  "Publication Date: 6 July 2023",
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: Patient Identification Rates",
    "Table 2: National Total",
    "Table 3: National Population",
    "Table 4: National Chemical Substance",
    "Table 5: ICB",
    "Table 6: Gender",
    "Table 7: Age Band",
    "Table 8: Age Band and Sex",
    "Table 9: Indices of Deprivation (IMD)",
    "Table 10: Monthly Section",
    "Table 11: Monthly Chemical Substance"
  ),
  c("Metadata", sheetNames)
)

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_bnf0411_Mar_23_test.xlsx",
                       overwrite = TRUE)