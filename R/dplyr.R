#' Connect to SQLServer or Sybase
#'
#' Use \code{src_sqlserver} to connect to an existing SQL Server or Sybase
#' database, and \code{tbl} to connect to tables within that database.
#'
#' @template sqlserver-parameters
#' @return a dplyr SQL based src with subclass \code{sqlserver}
#' @examples
#' \dontrun{
#' library(dplyr)
#' # Connection basics ---------------------------------------------------------
#' # To connect to TEST database, assumed to be specified in your ~/sql.yaml
#' # file (see \code{\link{have_test_server}}), first create a src:
#' my_src <- src_sqlserver("TEST")
#' # Then reference a tbl within that src
#' my_tbl <- tbl(my_src, "my_table")
#' # Methods -------------------------------------------------------------------
#' # You can then inspect table and perform actions on it
#' dim(my_tbl)
#' colnames(my_tbl)
#' head(my_tbl)
#' # Data manipulation verbs ---------------------------------------------------
#' filter(my_tbl, this.field == "that.value")
#' select(my_tbl, from.this.field:to.that.field)
#' arrange(my_tbl, this.field)
#' mutate(my_tbl, squared.field = field ^ 2)
#' # Group by operations -------------------------------------------------------
#' by_field <- group_by(my_tbl, field)
#' group_size(by_field)
#' by_field %>% summarise(ave = mean(numeric.field))
#' # See dplyr documentation for further information on data operations
#' }
#' @export
src_sqlserver <- function(host = NULL, database = NULL, user = NULL, password = NULL, ...) {
  if (!requireNamespace("odbc", quietly = TRUE)) {
    stop("odbc package required to connect to SQL Server db", call. = FALSE)
  }
  driver='ODBC Driver 13 for SQL Server'
  con <- DBI::dbConnect(
    odbc::odbc(),
    driver = 'ODBC Driver 13 for SQL Server',
    database = database,
    uid = user,
    pwd = password,
    server = host,
    port = 1433, ...
  )
  src <- dplyr::src_dbi(con, auto_disconnect = TRUE)
  newclass <- c('src_sqlserver', class(src))
  class(src) <- newclass
  src
}

#' @importFrom dplyr copy_to
#' @export
copy_to.src_sqlserver <- function (dest, df, name = deparse(substitute(df)), overwrite = FALSE, 
                                        types = NULL, temporary = TRUE, unique_indexes = NULL, indexes = NULL, 
                                        analyze = TRUE, ...) {
  if (temporary) {
    name = if (substr(name, 1, 1) != '#') name <- paste0('#', name)
    temporary = FALSE
  }
  NextMethod(
    dest, 
    df, 
    name = name, 
    overwrite = overwrite, 
    types = types, 
    temporary=temporary, 
    unique_indexes = unique_indexes, 
    indexes = indexes, 
    analyze = analyze,
    ...)
}

# Adds a custom tbl_sqlserver class to the tbl class
#' @importFrom dplyr tbl
#' @export
tbl.src_sqlserver <- function(src, from, ...) {
  dbi_tbl <- NextMethod()
  newclass <- c('tbl_sqlserver', class(dbi_tbl))
  class(dbi_tbl) <- newclass
  dbi_tbl
}

#' @importFrom dplyr compute
#' @export
compute.tbl_sql <- function(x, name = random_table_name(), temporary = TRUE,
                            unique_indexes = list(), indexes = list(),
                            ...) {
  if (temporary) {
    name = if (substr(name, 1, 1) != '#') name <- paste0('#', name)
    temporary = FALSE
  } 
  NextMethod(
    x, 
    name = name, 
    temporary = temporary,
    unique_indexes = unique_indexes, 
    indexes = indexes, 
    ...
  )
}

#' @importFrom dplyr db_data_type
#' @export
db_data_type.tbl_sql <- function (con, fields) 
{
  # SQL types --------------------------------------------------------------
  
  char_type <- function (x, obj) {
    # SQL Server 2000 does not support nvarchar(max) type.
    # TEXT is being deprecated. Make sure SQL types are UNICODE variants
    # (prefixed by N).
    # https://technet.microsoft.com/en-us/library/aa258271(v=sql.80).aspx
    n <- max(max(nchar(as.character(x), keepNA = FALSE)), 1)
    if (n > 4000) {
      if (sqlserver_version(obj) < 9) {
        n <- "4000"
      } else {
        n <- "MAX"
      }
    }
    paste0("NVARCHAR(", n, ")")
  }
  
  binary_type <- function (x, obj) {
    # SQL Server 2000 does not support varbinary(max) type.
    n <- max(max(nchar(x, keepNA = FALSE)), 1)
    if (n > 8000) {
      if (sqlserver_version(obj) < 9) {
        # https://technet.microsoft.com/en-us/library/aa225972(v=sql.80).aspx
        n <- "8000"
      } else {
        n <- "MAX"
      }
    }
    paste0("VARBINARY(", n, ")")
  }
  
  date_type <- function (x, obj) {
    if (sqlserver_version(obj) < 10) {
      # DATE available in >= SQL Server 2008 (>= v.10)
      "DATETIME"
    } else {
      "DATE"
    }
  }
  
  as_is_type <- function(x, obj) {
    class(x) <- class(x)[-match("AsIs", class(x))]
    dbDataType(obj, x)
  }
  
  data_frame_data_type <- function(x, obj) {
    vapply(x, dbDataType, FUN.VALUE = character(1), dbObj = obj, USE.NAMES = TRUE)
  }
  
  # Apply SQL types functions ------------------------------------------------
  
  vapply(fields, dbDataType, dbObj = con, FUN.VALUE = character(1))
}

