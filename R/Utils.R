#' Get server details from YAML file
#'
#' The \code{sql.yaml} file in a user's \code{HOME} directory can store
#' server details and login credentials (in plaintext). This works around
#' the instability associated with jTDS's single-sign on functionality.
#' The YAML file format is documented in this package's \code{README} file, while
#' an example is provided in \code{extdata/sql.yaml} (see example). At a
#' high level, each server should be documented in its own associative array
#' with each aspect of the server documented in an associative array.
#'
#' @param server corresponds to the server name key in the YAML \code{file} and
#' should be a string.
#' @param file defaults to \code{NULL} which means that it will use
#' \code{$HOME/sql.yaml}.
#' @return a named list of \code{server} details if this is specified in the
#' \code{file}. It stops and returns an error if \code{port} and \code{type}
#' keys are not specified for found \code{server}. \code{NULL} is returned if
#' the \code{file} does not contain the \code{server} key
#' @examples
#' # See link below
#' \dontrun{
#' aw <- dbConnect(RSQLServer::SQLServer(), server = "mhknbn2kdz.database.windows.net",
#'  database = 'AdventureWorks2012',
#'  properties = list(user = "sqlfamily", password = "sqlf@@m1ly"))
#' dbListTables(aw)
#' }
#' @seealso
#' \href{https://github.com/imanuelcostigan/RSQLServer/blob/master/README.md}{RSQLServer README}
#' \href{https://github.com/yaml/yaml}{YAML}
#' \href{http://sqlblog.com/blogs/jamie_thomson/archive/2012/03/27/adventureworks2012-now-available-to-all-on-sql-azure.aspx}{Example SQL Server instance}
#' @keywords internal

get_server_details <- function (server, file = NULL) {
  assertthat::assert_that(assertthat::is.string(server))
  server_details <- yaml::yaml.load_file(file)
  if (assertthat::has_name(server_details, server)) {
    server_detail <- server_details[[server]]
    assertthat::assert_that(!is.null(server_detail$port),
      !is.null(server_detail$type))
    return(server_detail)
  } else {
    return(NULL)
  }
}

sqlserver_version <- function (dbObj, ...) {
  parse_version <- function(odbc_con) {
    version_str <- dbGetInfo(odbc_con)$db.version
    as.integer(strsplit(version_str, '.', TRUE)[[1]][1])
  }
  version <- switch(class(dbObj)[1],
                    OdbcDriver = 0,
                    OdbcConnection = parse_version(dbObj),
                    OdbcResult = parse_version(dbObj@connection),
                    error("Unknown DBIObject", call. = FALSE)
  )
  if (version == 0) warning("Unknown SQL Server version. Try calling 'sqlserver_version' on a OdbcConnection or OdbcResult object.")
  return(version)
}