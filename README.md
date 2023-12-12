# medicines-used-in-mental-health

This code is published as part of the NHSBSA Official Statistics team's commitment to open code and transparency in how we produce our publications. The Medicines Used in Mental Health (MUMH) reproducible analytical pipeline (RAP) is owned and maintained by the Official Statistics team.

## Introduction

This RAP aims to bring together all code needed to run a pipeline in R to produce the latest [MUMH publication](https://www.nhsbsa.nhs.uk/statistical-collections/medicines-used-mental-health-england). It includes accompanying documentation in line with RAP best practice. 

The RAP includes a `functions` folder containing several files with functions specific to this publication, as well as a `sql` folder containing SQL code for extracting the raw data used by the pipeline. The RAP will produce an HTML report and accompanying HTML background and methodology document. This RAP makes use of many R packages, including several produced internally at the NHSBSA. Therefore, some of these packages cannot be accessed by external users. 

This RAP cannot be run in its entirety by external users. However it should provide information on how the Official Statistics team extract the data from the NHSBSA data warehouse, analyse the data, and produce the outputs released on the NHSBSA website as part of this publication.

This RAP is a work in progress and may be replaced as part of updates and improvements for each new release of the MUMH publication. The functions in the `functions` folder do not contain unit testing, although we will investigate adding this in future. These functions have undergone an internal peer review process.

## Getting started

You can clone the repository containing the RAP through [GitHub](https://github.com/) using the following steps.

In RStudio, click on "New project", then click "Version Control" and select the "Git" option.

Click "Clone Git Repository" then enter the URL of the mumh GitHub repository (https://github.com/nhsbsa-data-analytics/medicines-used-in-mental-health). You can click "Browse" to control where you want the cloned repository to be saved in your computer.

You will also need to create a [PAT key](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens).

You can view the [source code for the mumh RAP](https://github.com/nhsbsa-data-analytics/medicines-used-in-mental-health) on GitHub.

## Running this RAP

Users outside of the Official Statistics team may not have the required access permissions to run all parts of this RAP. The following information is included to document how this pipeline is run by members of the Official Statistics team during production.

Once the repository has been cloned, open the `pipeline.R` file and run the script from start to finish. You will be automatically prompted to enter your username and password into your .Renviron file, if not already there, to connect to the data warehouse. You will also be prompted to enter a schema name to extract data from. All other code in this script should require no other manual intervention.

The code should handle installing and loading any required packages and external data. It should then get data extracts from the fact table, perform data manipulations, then save this data into spreadsheet outputs. The pipeline will then render the statistical summary narratives and background document as HTML files for use in web publishing.

Publication details such as financial year and publication date can be manually changed in the `config.yml` file if this pipeline is rerun in future. Running the pipeline for a different time period may require users to change some function arguments, such as the fact table to extract data from.

## Functions guide

Functions used specifically for this RAP can be found in the [functions folder](https://github.com/nhsbsa-data-analytics/medicines-used-in-mental-health/tree/main/functions). The RAP also makes use of functions from a range of packages. A list of packages used is included at the beginning of the `pipeline.R` file, and installed and loaded within the pipeline code. Some functions contained in the functions folder may be placed into the internal NHSBSA packages in future.

Functions written directly for MUMH have been split into several R script files. Below is a guide to the functions each file contains.

1. `extract_functions.R` contains functions for getting the required data out of the fact table. They generally use the dbplyr package to interact with the NHSBSA data warehouse. 

Functions include `capture_rate_extract_period()`, `national_extract_period()`, `paragraph_extract_period()`, `chem_sub_extract_period()`, `icb_extract_period()`, `ageband_extract_period()`, `gender_extract_period()`, `age_gender_extract_period()`, `imd_extract_period()` and `child_adult_extract()`.

2. `population_functions.R` contains functions for extracting and manipulating population data for use in the pipeline.

Functions include `population_extract()` and `national_pop_agegen()`.

3. `vis_functions.R` contains functions for use in data visualisation for MUMH outputs, such as creating charts and formatting in markdown outputs. 

Functions include `age_gender_chart()`, `covid_chart_hc()`, `group_chart_hc_new()`, `infoBox_border()`, `infoBox_no_border()`, and `get_download_button()`.

4. `model_functions.R` contains functions for building the linear regression model of pre-COVID-19 pandemic trends.

Functions include `ageband_manip_20yr()`, `covid_lm()`, `fast_agg_pred()`, `month_pred_fun()`, and `prediction_list()`.

5. `apply_sdc.R` contains the `apply_sdc()` function to apply statistical disclosure control (SDC) to data in MUMH spreadsheet outputs. This is done in line with our [statistical disclosure control protocol](https://www.nhsbsa.nhs.uk/policies-and-procedures).


# Contributing

Contributions are not currently being accepted for this RAP. If this changes, a contributing guide will be made available.

# License

The `Medicines-used-in-Mental-Health` RAP, including associated documentation, is released under the MIT license. Details can be found in the `LICENSE` file.
