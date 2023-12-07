#function to apply statistical disclosure control (SDC)
#applies to total identified patients, total items, total NIC columns 
#user can set level to apply masking to, and set masking value
#defaults to a level of fewer than 5 identified patients and masks with NA 
#example: apply_sdc()

function(data,
         level = 5,
         rounding = TRUE,
         round_val = 5,
         mask = NA) {
  `%>%` <- magrittr::`%>%`
  
  rnd <- round_val
  
  if (is.character(mask)) {
    type <- function(x)
      as.character(x)
  } else {
    type <- function(x)
      x
  }
  
  data %>% dplyr::mutate(dplyr::across(
    where(is.numeric),
    .fns = ~ dplyr::case_when(
      .x >= level & rounding == T ~ type(rnd * round(.x / rnd)),
      .x < level & .x > 0 & rounding == T ~ mask,
      .x < level & .x > 0 & rounding == F ~ mask,
      TRUE ~ type(.x)
    ),
    .names = "sdc_{.col}"
  )) %>% dplyr::mutate(`sdc_Total Net Ingredient Cost (GBP)` = (dplyr::case_when(
    `sdc_Total Items` >= 5 ~ `sdc_Total Net Ingredient Cost (GBP)`,
    TRUE ~ mask)
  ))
}