#'
#'
#'

fathom_csv <- "NexTrak-R1.802463.2025-07-02.152320.csv"


read_fathom <- function(
  fathom_csv,
  data_type = "DET",
  preprocess = FALSE
) {
  # Convert data_type to uppercase
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
    # Read by skipping to the data using "<data_type>,"
    #   Select only the necessary columns
    #   Key by the first column for fast subsetting
    selected_data <- data.table::fread(
      file = fathom_csv,
      skip = paste0(data_type, ","),
      header = FALSE,
      fill = TRUE,
      select = seq_along(the_names),
      col.names = the_names,
      key = the_names[1]
    )[
      # Subset to the desired data type
      data_type
    ]
    data.table::setkey(selected_data, NULL)

    # Get ready to write to a temporary file to leverage data.table::fread's
    #   fast type detection and conversion
    tf <- tempfile()
    on.exit(unlink(tf))

    # Most data types have date/times only in the second and third columns
    #   Cheat later by writing a subset to a temporary file, grabbing the
    #   classes, and converting them directly.
    # Some data types have random other date/time columns. Since they are
    #   smaller, we'll read/write the whole thing.

    ## TODO: check if it is faster to convert in R

    if (
      data_type %in%
        c(
          "CLOCK_REF",
          "CLOCK_SET",
          "EVENT_INIT",
          "EVENT_OFFLOAD",
          "HEALTH_VR2W",
          "HEALTH_VR2AR",
          "HEALTH_HR2",
          "HEALTH_VR2TX"
        )
    ) {
      data.table::fwrite(
        selected_data,
        file = tf,
        logical01 = TRUE
      )
      selected_data <- data.table::fread(tf)
    } else {
      # Use the cheat noted above for other (possibly larger) data types
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
    }

    selected_data[]
  }
}
