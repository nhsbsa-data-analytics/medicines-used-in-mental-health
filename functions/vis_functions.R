## Chart functions and other visualisation related functions 
#for use in R markdown outputs of summary narratives
#some chart functions only used in outputs for annual publication 

#age_gender_chart() ------------------------------------------------------------
#function to create pyramid chart of ageband and gender groups, fills values for
#groups where no data present so these groups appear in final chart
#example: age_gender_chart()

age_gender_chart <- function(data,
                             labels = FALSE) {
  age_gender_chart_data <- data |>
    dplyr::select(`Age Band`,
                  `Patient Gender`,
                  `Total Identified Patients`) |>
    tidyr::complete(`Patient Gender`,
                    `Age Band`,
                    fill = list(`Total Identified Patients` = 0))
  
  categories = c(unique(age_gender_chart_data$`Age Band`))
  
  max <- max(age_gender_chart_data$`Total Identified Patients`)
  min <- max(age_gender_chart_data$`Total Identified Patients`) * -1
  
  male <- age_gender_chart_data |>
    dplyr::filter(`Patient Gender` == "Male")
  
  female <- age_gender_chart_data |>
    dplyr::filter(`Patient Gender` == "Female") |>
    dplyr::mutate(`Total Identified Patients` = 0 - `Total Identified Patients`)
  
  hc <- highcharter::highchart() |>
    highcharter::hc_chart(type = 'bar') |>
    hc_chart(style = list(fontFamily = "Arial")) |>
    highcharter::hc_xAxis(
      list(
        title = list(text = "Age group"),
        categories = categories,
        reversed = FALSE,
        labels = list(step = 1)
      ),
      list(
        categories = categories,
        opposite = TRUE,
        reversed = FALSE,
        linkedTo = 0,
        labels = list(step = 1)
      )
    ) |>
    highcharter::hc_tooltip(
      shared = FALSE,
      formatter = JS(
        "function () {
                   return this.point.category + '<br/>' +
                   '<b>' + this.series.name + '</b> ' +
                   Highcharts.numberFormat(Math.abs(this.point.y), 0);}"
      )
    ) |>
    highcharter::hc_yAxis(
      title = list(text = "Identified patients"),
      max = max,
      min = min,
      labels = list(
        formatter = JS(
          'function () {
               result = Math.abs(this.value);
               if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
               return result;
             }'
        )
      )
    ) |>
    highcharter::hc_plotOptions(series = list(stacking = 'normal')) |>
    highcharter::hc_series(
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#8e5300',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#8e5300",
        fontFamily = "Ariel",
        name = 'Male',
        data = c(male$`Total Identified Patients`)
      ),
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#003087',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y * -1;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return result.toPrecision(3) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return result.toPrecision(3) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#003087",
        name = 'Female',
        fontFamily = "Ariel",
        data = c(female$`Total Identified Patients`)
      )
    ) |>
    highcharter::hc_legend(reversed = T) |>
    highcharter::hc_credits(enabled = TRUE)
  
  return(hc)
  
}

#age_gender_chart_no_fill() ----------------------------------------------------
#function to created pyramid bar chart of ageband and gender groups without fill
#for use with BNF sections such as 0411 drugs for dementia where chart needs to 
#aggregate or omit ageband and gender groups with low or no identified patients
#example: age_gender_chart_no_fill()

age_gender_chart_no_fill <- function(data,
                                     labels = FALSE) {
  age_gender_chart_data <- data %>%
    dplyr::select(`Age Band`,
                  `Patient Gender`,
                  `Total Identified Patients`)
  
  categories = c(unique(age_gender_chart_data$`Age Band`))
  
  max <- max(age_gender_chart_data$`Total Identified Patients`)
  min <- max(age_gender_chart_data$`Total Identified Patients`) * -1
  
  male <- age_gender_chart_data %>%
    dplyr::filter(`Patient Gender` == "Male")
  
  female <- age_gender_chart_data %>%
    dplyr::filter(`Patient Gender` == "Female") %>%
    dplyr::mutate(`Total Identified Patients` = 0 - `Total Identified Patients`)
  
  hc <- highcharter::highchart() %>%
    highcharter::hc_chart(type = 'bar') %>%
    hc_chart(style = list(fontFamily = "Arial")) %>%
    highcharter::hc_xAxis(
      list(
        title = list(text = "Age group"),
        categories = categories,
        reversed = FALSE,
        labels = list(step = 1)
      ),
      list(
        categories = categories,
        opposite = TRUE,
        reversed = FALSE,
        linkedTo = 0,
        labels = list(step = 1)
      )
    ) %>%
    highcharter::hc_tooltip(
      shared = FALSE,
      formatter = JS(
        "function () {
                   return this.point.category + '<br/>' +
                   '<b>' + this.series.name + '</b> ' +
                   Highcharts.numberFormat(Math.abs(this.point.y), 0);}"
      )
    ) %>%
    highcharter::hc_yAxis(
      title = list(text = "Identified patients"),
      max = max,
      min = min,
      labels = list(
        formatter = JS(
          'function () {
               result = Math.abs(this.value);
               if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
               return result;
             }'
        )
      )
    ) %>%
    highcharter::hc_plotOptions(series = list(stacking = 'normal')) %>%
    highcharter::hc_series(
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#8e5300',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return Math.round(result.toPrecision(3)) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return Math.round(result.toPrecision(3)) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#8e5300",
        fontFamily = "Ariel",
        name = 'Male',
        data = c(male$`Total Identified Patients`)
      ),
      list(
        dataLabels = list(
          enabled = labels,
          inside = FALSE,
          color = '#003087',
          fontFamily = "Ariel",
          formatter = JS(
            'function () {
                                  result = this.y * -1;
                                  if (result >= 1000000) {result = result / 1000000;
                                                            return result.toPrecision(3) + "M"}
                                  else if (result >= 1000) {result = result / 1000;
                                                              return result.toPrecision(3) + "K"}
                                  return result;
                                  }'
          )
        ),
        color = "#003087",
        name = 'Female',
        fontFamily = "Ariel",
        data = c(female$`Total Identified Patients`)
      )
    ) %>%
    highcharter::hc_legend(reversed = T)
  
  return(hc)
  
}

#covid_chart_hc() --------------------------------------------------------------
#function to create time series line chart of model output data
#includes visualising 99% prediction interval on chart
#example: covid_chart_hc()

covid_chart_hc <- function(data,
                           title = NULL) {
  chart_data <- data %>%
    dplyr::mutate(
      ACT = prettyNum(signif(TOTAL_ITEMS, 3), big.mark = ","),
      EXP = prettyNum(signif(MEAN_FIT, 3), big.mark = ","),
      RANGE_95 = paste(
        formatC(
          signif(PILWR, 3),
          big.mark = ",",
          format = "f",
          digits = 0
        ),
        "-",
        formatC(
          signif(PIUPR, 3),
          big.mark = ",",
          format = "f",
          digits = 0
        )
      ),
      RANGE_99 = paste(
        formatC(
          signif(PILWR99, 3),
          big.mark = ",",
          format = "f",
          digits = 0
        ),
        "-",
        formatC(
          signif(PIUPR99, 3),
          big.mark = ",",
          format = "f",
          digits = 0
        )
      ),
      MONTH_START = as.Date(paste0(YEAR_MONTH_STRING, "01"), format = "%Y%m%d")
    )
  
  
  chart <- highchart() %>%
    highcharter::hc_chart(style = list(fontFamily = "Arial")) %>%
    highcharter::hc_add_series(
      data = chart_data,
      name = "99% prediction interval",
      type = "arearange",
      lineWidth = 0,
      color = "#768692",
      marker = list(enabled = FALSE),
      dataLabels = list(enabled = FALSE),
      # enableMouseTracking = FALSE,
      highcharter::hcaes(
        x = MONTH_START,
        high = signif(PIUPR99, 3),
        low = signif(PILWR99, 3),
        tooltip = RANGE_99
      )
    ) %>%
    #highcharter::hc_add_series(
    #data = chart_data,
    #name = "95% prediction interval",
    #type = "arearange",
    #lineWidth = 0,
    #color = "#b3bbc1",
    #marker = list(enabled = FALSE),
    #dataLabels = list(enabled = FALSE),
    #hcaes(
    #x = MONTH_START,
    #high = signif(PIupr, 3),
  #low = signif(PIlwr, 3),
  #tooltip = RANGE_95
  #)
  #) %>%
  highcharter::hc_add_series(
    data = chart_data,
    name = "Expected items",
    type = "line",
    dashStyle = "Dash",
    color = "#231f20",
    marker = list(enabled = FALSE),
    dataLabels = list(enabled = FALSE),
    hcaes(
      x = MONTH_START,
      y = signif(MEAN_FIT, 3),
      tooltip = EXP
    )
  ) %>%
    highcharter::hc_add_series(
      data = chart_data,
      name = "Prescribed items",
      type = "line",
      lineWidth = 3,
      color = "#003087",
      marker = list(enabled = FALSE),
      dataLabels = list(enabled = FALSE),
      hcaes(
        x = MONTH_START,
        y = signif(TOTAL_ITEMS, 3),
        tooltip = ACT
      )
    ) %>%
    highcharter::hc_xAxis(
      type = "datetime",
      dateTimeLabelFormats = list(month = "%b %y"),
      title = list(text = "Month")
    ) %>%
    highcharter::hc_yAxis(title = list(text = "Volume"),
                          min = 0) %>%
    highcharter::hc_title(text = title,
                          style = list(fontSize = "16px",
                                       fontWeight = "bold")) %>%
    highcharter::hc_legend(enabled = TRUE,
                           reversed = TRUE) %>%
    highcharter::hc_tooltip(
      enabled = TRUE,
      shared = TRUE,
      useHTML = TRUE,
      formatter = JS(
        "function () {
        var timeStamp = this.x;
        var dateFormat = new Date(timeStamp);
        var month = dateFormat.toLocaleString('default', { month: 'long' });
        var year = dateFormat.getFullYear();

        var s = month + ' ' + year;

        $.each(this.points.reverse(), function () {
            var number = this.point.tooltip;

            s += '<br/><span style=\"color:' + this.series.color + '\">\u25CF</span> ' + this.series.name + ': ' +
                '<b>' + number + '</b>';
        });

        return s;
    }"
      )
    ) %>%
    highcharter::hc_credits(enabled = TRUE) %>%
    highcharter::hc_plotOptions(arearange = list(states = list(hover = list(enabled = FALSE))))
  
  
  # explicit return
  return(chart)
  
}

#group_chart_hc_new() ----------------------------------------------------------
#function to create group bar chart with NHS colour scheme and formatted numbers
#using highcharter package. Function name specifies 'new' to avoid clashes with
#similarly named functions in R packages also used in pipeline
#example: group_chart_hc_new(data = iris,
#                             x = "Sepal.Length",
#                             y = "Petal.Width",
#                             group = "Species",
#                             type = "scatter")

group_chart_hc_new <- function(data,
                               x,
                               y,
                               type = "line",
                               group,
                               xLab = NULL,
                               yLab = NULL,
                               title = NULL,
                               dlOn = TRUE,
                               currency = FALSE,
                               marker = TRUE) {
  # this function creates a group bar chart with NHSBSA data vis standards
  # applied. includes datalabel formatter to include "£" if needed.
  
  x <- rlang::enexpr(x)
  y <- rlang::enexpr(y)
  
  group <- rlang::enexpr(group)
  
  # set font to arial
  font <- "Arial"
  
  # get number of groups. max number of groups is 9 for unique colors
  num_groups <- length(unique(data[[group]]))
  
  # define a set of colors
  colors <- c("#03045E", "#0077B6", "#00B4D8", "#90E0EF", "#CAF0F8")
  
  # if there are more groups than colors, recycle the colors
  if (num_groups > length(colors)) {
    colors <- rep(colors, length.out = num_groups)
  }
  
  
  #if there is a 'Total' groups ensure this takes the color black
  if ("Total" %in% unique(data[[group]])) {
    #identify index of "total" group
    total_index <- which(sort(unique(data[[group]])) == "Total")
    
    # add black to location of total_index
    colors <-
      c(colors[1:total_index - 1], "#000000", colors[total_index:length(colors)])
  }
  
  # subset the colors to the number of groups
  #colors <- ifelse(unique(data[[group]]) == "Total", "black", colors[1:num_groups])
  
  # check currency argument to set symbol
  dlFormatter <- highcharter::JS(
    paste0(
      "function() {
    var ynum = this.point.y;
    var options = { maximumSignificantDigits: 3, minimumSignificantDigits: 3 };
      if (",
      tolower(as.character(currency)),
      ") {
      options.style = 'currency';
      options.currency = 'GBP';
      }
      if (ynum >= 1000000000) {
        options.maximumSignificantDigits = 4;
        options.minimumSignificantDigits = 4;
      }else {
       options.maximumSignificantDigits = 3;
        options.minimumSignificantDigits = 3;
      }
    return ynum.toLocaleString('en-GB', options);
  }"
    )
  )
  
  
  # ifelse(is.na(str_extract(!!y, "(?<=\\().*(?=,)")),!!y,str_extract(!!y, "(?<=\\().*(?=,)")),
  
  # check chart type to set grid lines
  gridlineColor <- if (type == "line")
    "#e6e6e6"
  else
    "transparent"
  
  # check chart type to turn on y axis labels
  yLabels <- if (type == "line")
    TRUE
  else
    FALSE
  
  # highchart creation
  chart <- highcharter::highchart() |>
    highcharter::hc_chart(style = list(fontFamily = font)) |>
    highcharter::hc_colors(colors) |>
    # add only series
    highcharter::hc_add_series(
      data = data,
      type = type,
      marker = list(enabled = marker),
      highcharter::hcaes(
        x = !!x,
        y = !!y,
        group = !!group
      ),
      groupPadding = 0.1,
      pointPadding = 0.05,
      dataLabels = list(
        enabled = dlOn,
        formatter = dlFormatter,
        style = list(textOutline = "none")
      )
    ) |>
    highcharter::hc_xAxis(type = "category",
                          title = list(text = xLab)) |>
    # turn off y axis and grid lines
    highcharter::hc_yAxis(
      title = list(text = yLab),
      labels = list(enabled = yLabels),
      gridLineColor = gridlineColor,
      min = 0
    ) |>
    highcharter::hc_title(text = title,
                          style = list(fontSize = "16px",
                                       fontWeight = "bold")) |>
    highcharter::hc_legend(enabled = TRUE) |>
    highcharter::hc_tooltip(enabled = FALSE) |>
    highcharter::hc_credits(enabled = TRUE)
  
  # explicit return
  return(chart)
}

#infoBox_border() --------------------------------------------------------------
#function to create info box in NHS colour scheme with border
#example: infobox_border(" ", text = "Text goes here", width = 100%)

infoBox_border <- function(
    header = "Header here",
    text = "More text here",
    backgroundColour = "#ccdff1",
    borderColour = "#005EB8",
    width = "31%",
    fontColour = "black") {
  paste(
    "<div class='infobox_border' style = 'border: 1px solid ", borderColour,"!important;
  border-left: 5px solid ", borderColour,"!important;
  background-color: ", backgroundColour,"!important;
  padding: 10px;
  margin-bottom: 20px;
  width: ", width,"!important;
  display: inline-block;
  vertical-align: top;
  flex: 1;
  height: 100%;'>
  <h4 style = 'color: ", fontColour, ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;'>", header, "</h4>
  <p style = 'color: ", fontColour, ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>", text, "</p>
</div>"
  )
}

#infoBox_no_border -------------------------------------------------------------
#function to create info box in NHS colour scheme without border
#example: infoBox_no_border("", text = "<b>Text goes here.</b>", width = "100%")

infoBox_no_border <- function(header = "Header here",
                              text = "More text here",
                              backgroundColour = "#005EB8",
                              width = "31%",
                              fontColour = "white") {
  #set handling for when header is blank
  display <- "block"
  
  if (header == "") {
    display <- "none"
  }
  
  paste(
    "<div class='infobox_no_border',
    style = 'background-color: ",
    backgroundColour,
    "!important;padding: 10px;
    width: ",
    width,
    ";
    display: inline-block;
    vertical-align: top;
    flex: 1;
    height: 100%;'>
  <h4 style = 'color: ",
    fontColour,
    ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ",
    display,
    ";'>",
    header,
    "</h4>
  <p style = 'color: ",
    fontColour,
    ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>",
    text,
    "</p>
</div>"
  )
}

#get_download_button() ---------------------------------------------------------
#function to create button to display in markdown file,
#assigning data used in chart to this button will download data as csv
#when button is clicked in html output file webpage
#example: get_download_button(title = "Download chart data", 
#                             data = figure_1_data, 
#                             filename = "figure_1")

get_download_button <-
  function(data = data,
           title = "Download chart data",
           filename = "data") {
    dt <- datatable(
      data,
      rownames = FALSE,
      extensions = 'Buttons',
      options = list(
        searching = FALSE,
        paging = TRUE,
        bInfo = FALSE,
        pageLength = 1,
        dom = '<"datatable-wrapper"B>',
        buttons = list(
          list(
            extend = 'csv',
            text = title,
            filename = filename,
            className = "nhs-button-style"
          )
        ),
        initComplete = JS(
          "function(settings, json) {",
          "$(this.api().table().node()).css('visibility', 'collapse');",
          "}"
        )
      )
    )
    
    return(dt)
  }