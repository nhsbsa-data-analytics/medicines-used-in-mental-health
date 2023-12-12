##Functions used to build and apply linear regression model of pre-COVID-19
#prescribing trends

#ageband_manip_20yr() ----------------------------------------------------------
#function takes data input with 5 year agebands and aggregates
#to 20 year agebands by gender for use as variables in model building
#also fills groups where no values recorded and creates time related variables
#example: ageband_manip_20yr(df)

ageband_manip_20yr <- function(data) {
  #aggregate from 5 year ageband to 20 year ageband
  data_20yr <- data %>%
    dplyr::mutate(
      BAND_20YR = dplyr::case_when(
        `Age Band` %in% c("00-04", "05-09", "10-14", "15-19") ~ "00-19",
        `Age Band` %in% c("20-24", "25-29", "30-34", "35-39") ~ "20-39",
        `Age Band` %in% c("40-44", "45-49", "50-54", "55-59") ~ "40-59",
        `Age Band` %in% c("60-64", "65-69", "70-74", "75-79") ~ "60-79",
        `Age Band` == "Unknown" ~ "Unknown",
        TRUE ~ "80+"
      )
    ) %>%
    dplyr::select(!(`Age Band`)) %>%
    dplyr::group_by(
      YEAR_MONTH = `Year Month`,
      SECTION_NAME = `BNF Section Name`,
      SECTION_CODE = `BNF Section Code`,
      IDENTIFIED_FLAG = `Identified Patient Flag`,
      PDS_GENDER = `Patient Gender`,
      BAND_20YR
    ) %>%
    dplyr::summarise(
      ITEM_COUNT = sum(`Total Items`),
      ITEM_PAY_DR_NIC = sum(`Total Net Ingredient Cost (GBP)`),
      .groups = "drop"
    ) %>%
    #fill in ageband and gender levels with 0 items
    tidyr::complete(
      BAND_20YR,
      nesting(
        YEAR_MONTH,
        SECTION_NAME,
        SECTION_CODE,
        IDENTIFIED_FLAG,
        PDS_GENDER
      ),
      fill = list(
        ITEM_COUNT = 0,
        ITEM_PAY_DR_NIC = 0,
        PATIENT_COUNT = 0
      )
    ) %>%
    tidyr::complete(
      IDENTIFIED_FLAG,
      nesting(
        YEAR_MONTH,
        SECTION_NAME,
        SECTION_CODE,
        BAND_20YR,
        PDS_GENDER
      ),
      fill = list(
        ITEM_COUNT = 0,
        ITEM_PAY_DR_NIC = 0,
        PATIENT_COUNT = 0
      )
    ) %>%
    tidyr::complete(
      PDS_GENDER,
      nesting(
        YEAR_MONTH,
        SECTION_NAME,
        SECTION_CODE,
        IDENTIFIED_FLAG,
        BAND_20YR
      ),
      fill = list(
        ITEM_COUNT = 0,
        ITEM_PAY_DR_NIC = 0,
        PATIENT_COUNT = 0
      )
    ) %>%
    dplyr::group_by(SECTION_NAME,
                    SECTION_CODE,
                    IDENTIFIED_FLAG,
                    PDS_GENDER,
                    BAND_20YR) %>%
    dplyr::group_by(SECTION_NAME,
                    SECTION_CODE,
                    IDENTIFIED_FLAG,
                    PDS_GENDER,
                    BAND_20YR) %>%
    #get month place in year, month since start of data, month placement
    #mutate to add as columns for use in later functions
    dplyr::mutate(
      MONTH_START = as.Date(paste0(YEAR_MONTH, "01"), format = "%Y%m%d"),
      MONTH_NUM = lubridate::month(MONTH_START),
      MONTH_INDEX = lubridate::interval(lubridate::dmy(01032015), as.Date(MONTH_START)) %/% months(1)
    ) %>%
    #join dispensing days for each month
    #remove patients with unknown ageband or gender
    dplyr::left_join(dispensing_days_data,
                     by = "YEAR_MONTH") %>%
    dplyr::filter(
      !(IDENTIFIED_FLAG == "N" &
          PDS_GENDER == "F"),
      !(IDENTIFIED_FLAG == "N" &
          PDS_GENDER == "M"),
      !(PDS_GENDER == "U" |
          BAND_20YR == "Unknown")
    ) %>%
    dplyr::ungroup() %>%
    #turn each level of MONTH_NUM column into 12 separate columns
    #for use as variables of month of the year in linear model
    dplyr::mutate(
      m_01 = 1 * (MONTH_NUM == 1),
      m_02 = 1 * (MONTH_NUM == 2),
      m_03 = 1 * (MONTH_NUM == 3),
      m_04 = 1 * (MONTH_NUM == 4),
      m_05 = 1 * (MONTH_NUM == 5),
      m_06 = 1 * (MONTH_NUM == 6),
      m_07 = 1 * (MONTH_NUM == 7),
      m_08 = 1 * (MONTH_NUM == 8),
      m_09 = 1 * (MONTH_NUM == 9),
      m_10 = 1 * (MONTH_NUM == 10),
      m_11 = 1 * (MONTH_NUM == 11),
      m_12 = 1 * (MONTH_NUM == 12)
    ) %>%
    ungroup() %>%
    #add column to show if month pre-covid or covid period onwards
    dplyr::mutate(time_period = case_when(YEAR_MONTH <= 202002 ~ "pre_covid",
                                          TRUE ~ "covid"))
  return(data_20yr)
}

#covid_lm() --------------------------------------------------------------------
#function creates linear regression model with previously selected variables
#model is built using specified training data 
#example: covid_lm(df_training,
#                   section_code = "0401")

covid_lm <- function(training_data,
                     section_code) {
  #create linear model using lm() function with all required variables
  #include age and gender as interaction term
  #include month position within year as separate columns
  covid_lm_with_months <-
    lm(
      ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + m_02 + m_03
      + m_04 + m_05 + m_06 + m_07 + m_08 + m_09 + m_10 + m_11 + m_12
      + as.factor(PDS_GENDER) * as.factor(BAND_20YR),
      data = filter(training_data, SECTION_CODE == section_code)
    )
  
  return(covid_lm_with_months)
  
}

#fast_agg_pred() ---------------------------------------------------------------
#function extrapolates predicted values, calculates mean, residual
#variance, confidence interval, and prediction interval for the aggregated value
#applies covid_lm() linear model to new data for making predictions on
#function is called inside month_pred_fun() so arguments set using month_pred_fun()
#example: fast_agg_pred(rep.int(1, nrow(data)),
#                       data,
#                       lmObject = model,
#                       alpha = 0.99)

fast_agg_pred <- function (w, lmObject, newdata, alpha = 0.95) {
  ## input checking
  
  if (!inherits(lmObject, "lm"))
    stop("'lmObject' is not a valid 'lm' object!")
  if (!is.data.frame(newdata))
    newdata <- as.data.frame(newdata)
  if (length(w) != nrow(newdata))
    stop("length(w) does not match nrow(newdata)")
  
  ## extract "terms" object from the fitted model, but delete response variable
  tm <- delete.response(terms(lmObject))
  
  ## linear predictor matrix
  Xp <- model.matrix(tm, newdata)
  
  ## predicted values by direct matrix-vector multiplication
  pred <- c(Xp %*% coef(lmObject))
  
  ## mean of the aggregation
  agg_mean <- c(crossprod(pred, w))
  
  ## residual variance
  sig2 <- c(crossprod(residuals(lmObject))) / df.residual(lmObject)
  
  ## efficiently compute variance of the aggregation without matrix-matrix computations
  
  QR <- lmObject$qr   ## qr object of fitted model
  piv <- QR$pivot     ## pivoting index
  r <- QR$rank        ## model rank / numeric rank
  
  u <- forwardsolve(t(QR$qr), c(crossprod(Xp, w))[piv], r)
  
  agg_variance <- c(crossprod(u)) * sig2
  
  ## adjusted variance of the aggregation
  agg_variance_adj <- agg_variance + c(crossprod(w)) * sig2
  
  ## t-distribution quantiles
  Qt <-
    c(-1, 1) * qt((1 - alpha) / 2, lmObject$df.residual, lower.tail = FALSE)
  
  ## names of CI and PI
  NAME <- c("lower", "upper")
  
  ## CI
  CI <- setNames(agg_mean + Qt * sqrt(agg_variance), NAME)
  
  ## PI
  PI <- setNames(agg_mean + Qt * sqrt(agg_variance_adj), NAME)
  
  ## return
  list(
    mean = agg_mean,
    var = agg_variance,
    CI = CI,
    PI = PI
  )
}

#month_pred_fun() --------------------------------------------------------------
#function calculates predicted values by month using linear model
#function calls fast_agg_pred() to pass arguments to
#user can set alpha, will default to 95 to calculate a 95% prediction interval
#a 99% prediction interval is also calculated 
#example: month_pred_fun()

month_pred_fun <- function(month, data, model, alpha = 0.95) {
  data <- data %>%
    dplyr::filter(YEAR_MONTH == month)
  
  pred <-
    fast_agg_pred(rep.int(1, nrow(data)),
                  data,
                  lmObject = model,
                  alpha = alpha)
  pred99 <-
    fast_agg_pred(rep.int(1, nrow(data)),
                  data,
                  lmObject = model,
                  alpha = 0.99)
  output <- data.frame(UNIT = 1)
  
  #use list of months within data
  #include columns for lower and upper 95% and 99% CI
  output$YEAR_MONTH <- month
  output$MEAN_FIT <- pred[["mean"]]
  output$VAR <- pred[["var"]]
  output$PILWR <- pred[["PI"]][["lower"]]
  output$PIUPR <- pred[["PI"]][["upper"]]
  output$PILWR99 <- pred99[["PI"]][["lower"]]
  output$PIUPR99 <- pred99[["PI"]][["upper"]]
  output$UNIT <- NULL
  
  return(output)
  
}

#prediction_list() -------------------------------------------------------------
#function to apply to selected BNF section drug groups
#predictions made for each month from March 2020 onwards
#example: prediction_list(df20,
#                         "0401",
#                         model_0401,
#                         pred_month_list)

prediction_list <- function(data,
                            section_code,
                            covid_lm_output,
                            pred_month_list) {
  pred_month_list <- df20 %>%
    dplyr::filter(YEAR_MONTH >= 202003) %>%
    pull(YEAR_MONTH) %>%
    unique()
  
  #get predictions based on BNF section code
  if (section_code == "0401") {
    df_0401 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #apply month_pred_fun() on each month within pred_month_list
    pred_0401 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0401,
                        model = covid_lm_output)
    
    unlist(pred_0401)
    
    rbindlist(pred_0401)
    
    #create final dataset by binding predictions onto actual items data
    #add YEAR_MONTH as character column for easier use in chart
    section_pred_list <- df_0401 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0401)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  else if (section_code == "0402") {
    df_0402 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #default PI of 95%
    pred_0402 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0402,
                        model = covid_lm_output)
    
    unlist(pred_0402)
    
    rbindlist(pred_0402)
    
    section_pred_list <- df_0402 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0402)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  else if (section_code == "0403") {
    df_0403 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #default PI of 95%
    pred_0403 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0403,
                        model = covid_lm_output)
    
    unlist(pred_0403)
    
    rbindlist(pred_0403)
    
    section_pred_list <- df_0403 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0403)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  else if (section_code == "0404") {
    df_0404 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #default PI of 95%
    pred_0404 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0404,
                        model = covid_lm_output)
    
    unlist(pred_0404)
    
    rbindlist(pred_0404)
    
    section_pred_list <- df_0404 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0404)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  else if (section_code == "0411") {
    df_0411 <- data %>%
      dplyr::filter(SECTION_CODE == section_code)
    
    #default PI of 95%
    pred_0411 <- lapply(pred_month_list,
                        month_pred_fun,
                        data = df_0411,
                        model = covid_lm_output)
    
    unlist(pred_0411)
    
    rbindlist(pred_0411)
    
    section_pred_list <- df_0411 %>%
      dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
      dplyr::summarise(TOTAL_ITEMS = sum(ITEM_COUNT)) %>%
      left_join(rbindlist(pred_0411)) %>%
      dplyr::mutate(YEAR_MONTH_STRING = as.character(YEAR_MONTH)) %>%
      ungroup()
  }
  
  return(section_pred_list)
  
}