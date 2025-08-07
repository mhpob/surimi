#'
#'
#'

fathom_csv <- "NexTrak-R1.802463.2025-07-02.152320.csv"


read_fathom <- function(
    fathom_csv,
    data_type = "DET",
    preprocess = FALSE) {
  data_type <- toupper(data_type)

  # Grab the column names from the respective "x_DESC" row
  the_names <- as.character(
    data.table::fread(
      fathom_csv,
      skip = paste0(data_type, "_DESC"),
      nrows = 1,
      header = FALSE
    )
  )

  # Use system-level pre-processing if requested
  if (isTRUE(preprocess)) {
    # Check OS; assumes FINDSTR on Windows and grep on other systems
    search_fun <- ifelse(
      .Platform$OS.type == "windows",
      "FINDSTR /l",
      "grep -F"
    )

    data.table::fread(
      cmd = paste(
        search_fun,
        paste0(data_type, ","),
        fathom_csv
      ),
      header = FALSE,
      col.names = the_names
    )
  } else {
    # Read the data by skipping to the data using "DET,", for example
    selected_data <- data.table::fread(
      file = fathom_csv,
      skip = paste0(data_type, ","),
      header = FALSE,
      fill = TRUE,
      select = seq_along(the_names),
      col.names = the_names,
      key = the_names[1]
    )[
      # Select only the necessary columns
      data_type
    ]

    tf <- tempfile()
    on.exit(unlink(tf))

    data.table::fwrite(
      head(selected_data, 100),
      file = tf,
      logical01 = TRUE
    )
    classes <- data.table::fread(tf)
    classes <- sapply(classes, class)

    for (col in names(classes)[4:length(classes)]) {
      data.table::set(
        selected_data,
        j = col,
        value = as(selected_data[[col]], unlist(classes[col]))
      )
    }
    data.table::setkey(selected_data, NULL)
    selected_data[]
  }
}
