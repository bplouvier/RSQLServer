#' @import DBI
NULL

#' @import dplyr
NULL

#' @importFrom dplyr db_has_table
#' @importFrom dbplyr build_sql escape
#' @export
db_has_table.OdbcConnection <- function (con, table) {
  if (substr(table, 1, 1) == '#') {
    object_query <- paste0('tempdb..', table)
  } else {
    object_query <- table
  }
  
  qry <- build_sql(
    "SELECT CASE WHEN OBJECT_ID(N", 
    escape(object_query),
    ", N'U') IS NULL THEN 0 ELSE 1 END AS table_found")
  
  res <- dbGetQuery(con, qry)
  if(nrow(res) != 1) {
    stop(paste0("Found ", nrow(res), " rows when checking if table ", table, " exists (expecting exactly one row)"))
  }
  invisible(res[[1]] == 1)
}

#' @importFrom dplyr db_save_query sql_subquery
#' @export
db_save_query.OdbcConnection <- function (con, sql, name, temporary = TRUE,
  ...) {
  # http://smallbusiness.chron.com/create-table-query-results-microsoft-sql-50836.html
  qry <- build_sql("SELECT * INTO ", ident(name), " FROM ",
    sql_subquery(con, sql), con = con)
  dbExecute(con, qry)
}

#' @importFrom dplyr db_drop_table
#' @export
db_drop_table.OdbcConnection <- function(con, table, force = FALSE, ...) {
  # Equivalent of if exists. When force = TRUE, the drop command will only execute if the table exists
  if (db_has_table(con, table) || !force) {
    sql <- build_sql("DROP TABLE ", ident(table), con = con)
    dbExecute(con, sql)
  } 
}

#' Todo: find a solution for storing rownames
#' @export
db_insert_into.OdbcConnection <- function(con, table, values, ...) {
  rownames(values) <- NULL
  DBI::dbWriteTable(con, table, values, append = TRUE)
}

#' @importFrom dplyr db_analyze
#' @export
db_analyze.OdbcConnection <- function (con, table, ...) {
  TRUE
}

# Inherited db_create_index.DBIConnection method from dplyr

#' @importFrom dplyr db_explain %>%
#' @export
db_explain.OdbcConnection <- function (con, sql, ...) {
  # SET SHOWPLAN_ALL available from SQL Server 2000 on.
  # https://technet.microsoft.com/en-us/library/aa259203(v=sql.80).aspx
  # http://msdn.microsoft.com/en-us/library/ms187735.aspx
  # http://stackoverflow.com/a/7359705/1193481
  dbSendStatement(con, "SET SHOWPLAN_ALL ON")
  on.exit(dbSendStatement(con, "SET SHOWPLAN_ALL OFF"))
  res <- dbGetQuery(con, sql) %>%
    dplyr::select_("StmtId", "NodeId", "Parent", "PhysicalOp", "LogicalOp",
      "Argument", "TotalSubtreeCost")
  paste(utils::capture.output(print(res)), collapse = "\n")
}

#' @importFrom dplyr sql_select
#' @importFrom dbplyr sql
#' @importFrom purrr %||%
#' @export
sql_select.OdbcConnection <- function (con, select, from, where = NULL,
                                       group_by = NULL, having = NULL, order_by = NULL, limit = NULL, distinct = FALSE, ...) {

  # REFERENCES --------------------------------------------------------------
  # 2000 : https://technet.microsoft.com/en-us/library/aa259187(v=sql.80).aspx
  # 2005+: https://msdn.microsoft.com/en-us/library/ms189499(v=sql.90).aspx

  # SETUP -------------------------------------------------------------------

  out <- vector("list", 10)
  names(out) <- c("select", "from", "where", "group_by", "having",
                  "order_by", "limit")

  # SELECT ------------------------------------------------------------------

  assertthat::assert_that(is.character(select), length(select) > 0L)

  if (length(limit) > 0L || length(order_by) > 0L) {
    # If ordering, then TOP should be specified, if it isn't already,
    # to ensure query works when query is part of a subquery. See #49
    # Code via Nick Kennedy:
    # https://github.com/imanuelcostigan/RSQLServer/pull/129#commitcomment-20230748
    if (length(limit) == 0L) {
      limit <- mssql_top(con, 100, TRUE)
    } else {
      limit <- mssql_top(con, limit, FALSE)
    }
  }

  assertthat::assert_that(is.character(select), length(select) > 0L)
  out$select <- build_sql("SELECT ",
                          if (distinct) sql("DISTINCT "), limit, " ",
                          escape(select, collapse = ", ", con = con), con = con)

  # FROM --------------------------------------------------------------------

  assertthat::assert_that(assertthat::is.string(from))
  out$from <- build_sql("FROM ", from, con = con)

  # WHERE -------------------------------------------------------------------

  if (length(where) > 0L) {
    assertthat::assert_that(is.character(where))
    out$where <- build_sql("WHERE ",
                           escape(where, collapse = " AND ", con = con))
  }

  # GROUP BY ----------------------------------------------------------------

  if (length(group_by) > 0L) {
    assertthat::assert_that(is.character(group_by), length(group_by) > 0L)
    out$group_by <- build_sql("GROUP BY ",
                              escape(group_by, collapse = ", ", con = con))
  }

  # HAVING ------------------------------------------------------------------

  if (length(having) > 0L) {
    assertthat::assert_that(assertthat::is.string(having))
    out$having <- build_sql("HAVING ",
                            escape(having, collapse = ", ", con = con))
  }

  # ORDER BY ----------------------------------------------------------------

  if (length(order_by) > 0L) {
    assertthat::assert_that(is.character(order_by), length(order_by) > 0L)
    out$order_by <- build_sql("ORDER BY ",
                              escape(order_by, collapse = ", ", con = con))
  }

  # Resulting SELECT --------------------------------------------------------

  escape(unname(compact(out)), collapse = "\n", parens = FALSE, con = con)
}


#' @importFrom dbplyr escape
mssql_top <- function (con, n, is_percent = NULL) {
  # https://technet.microsoft.com/en-us/library/aa259187(v=sql.80).aspx
  # https://msdn.microsoft.com/en-us/library/ms189463(v=sql.90).aspx
  assertthat::assert_that(assertthat::is.number(n), n >= 0)
  n <- as.integer(n)
  is_mssql_2000 <- sqlserver_version(con) == 8
  if (is.null(is_percent) || !isTRUE(is_percent)) {
    if (!is_mssql_2000) n <- escape(n, parens = TRUE)
    return(build_sql("TOP ", n))
  } else {
    # Assume TOP n PERCENT. n must already be >= 0
    assertthat::assert_that(n <= 100)
    if (!is_mssql_2000) n <- escape(n, parens = TRUE)
    return(build_sql("TOP ", n, " PERCENT"))
  }
}

## Math (scalar) functions - no change across versions based on eyeballing:
# MSSQL 2000: https://technet.microsoft.com/en-us/library/aa258862(v=sql.80).aspx
# MSSQL 2005: https://technet.microsoft.com/en-us/library/ms177516(v=sql.90).aspx
# MSSQL 2008: https://technet.microsoft.com/en-us/library/ms177516(v=sql.100).aspx
# MSSQL 2008(r2): https://technet.microsoft.com/en-us/library/ms177516(v=sql.105).aspx
# MSSQL 2012: https://technet.microsoft.com/en-us/library/ms177516(v=sql.110).aspx

## Aggregate functions
# MSSQL 2005: https://technet.microsoft.com/en-US/library/ms173454(v=sql.90).aspx
# MSSQL 2008: https://technet.microsoft.com/en-US/library/ms173454(v=sql.100).aspx
# MSSQL 2008r2*: https://technet.microsoft.com/en-US/library/ms173454(v=sql.100).aspx
# MSSQL 2012*: https://technet.microsoft.com/en-US/library/ms173454(v=sql.110).aspx
# MSSQL 2014: https://technet.microsoft.com/en-US/library/ms173454(v=sql.120).aspx
#' @importFrom dplyr sql_translate_env
#' @importFrom dbplyr base_agg base_scalar sql_prefix base_win sql_variant sql_translator
#' @export
sql_translate_env.OdbcConnection <- function (con) {
  sql_variant(
    scalar = sql_translator(.parent = base_scalar,
                            # http://sqlserverplanet.com/tsql/format-string-to-date
                            as.POSIXct = function(x) build_sql("CAST(", x, " AS DATETIME)"),
                            # DATE data type only available since SQL Server 2008
                            as.Date = function (x) build_sql("CAST(", x, " AS DATE)"),
                            as.numeric = function(x) build_sql("CAST(", x, " AS FLOAT)"),
                            as.character = function(x) build_sql("CAST(", x, " AS NVARCHAR(4000))")
    ),
    aggregate = sql_translator(.parent = base_agg,
                               n = function() sql("COUNT(*)"),
                               mean = sql_prefix('AVG'),
                               sd = sql_prefix("STDEV"),
                               sdp = sql_prefix("STDEVP"),
                               varp = sql_prefix("VARP")
    ),
    window = base_win
  )
}

#' @importFrom dplyr db_begin
#' @export
db_begin.OdbcConnection <- function(con) {
  invisible(TRUE)
}

#' @importFrom dplyr db_commit
#' @export
db_commit.OdbcConnection <- function(con) {
  invisible(TRUE)
}

#' @importFrom dplyr db_rollback
#' @export
db_rollback.OdbcConnection <- function(con) {
  invisible(TRUE)
}

#' @export
#' @importFrom dbplyr db_copy_to
#' @importFrom dplyr db_write_table db_data_type
db_copy_to.SQLServerConnection <- function(con, table, values,
                                           overwrite = FALSE, types = NULL, temporary = TRUE,
                                           unique_indexes = NULL, indexes = NULL,
                                           analyze = TRUE, ...) {
  
  # Modified version of dbplyr method.
  # SQL Server doesn't have ANALYZE TABLE support so this part of
  # db_copy_to() has been dropped
  
  types <- types %||% db_data_type(con, values)
  names(types) <- names(values)
  if (overwrite) {
    db_drop_table(con, table, force = TRUE)
  }
  db_write_table(con, table, types = types, values = values,
                 temporary = temporary)
  db_create_indexes(con, table, unique_indexes, unique = TRUE)
  db_create_indexes(con, table, indexes, unique = FALSE)
  table
}

#' @importFrom dplyr db_create_indexes
#' @importFrom dbplyr db_compute
#' @export
db_compute.SQLServerConnection <- function(con, table, sql, temporary = TRUE,
                                           unique_indexes = list(), indexes = list(), ...) {
  # Modified from dbplyr because db_save_query returns a temp table name which
  # must be used by subsequent method calls.
  if (!is.list(indexes)) {
    indexes <- as.list(indexes)
  }
  if (!is.list(unique_indexes)) {
    unique_indexes <- as.list(unique_indexes)
  }
  
  table <- db_save_query(con, sql, table, temporary = temporary)
  db_create_indexes(con, table, unique_indexes, unique = TRUE)
  db_create_indexes(con, table, indexes, unique = FALSE)
  
  table
}
