#' Create a queue
#'
#' Create a new queue along with necessary files
#'
#'
#'
#'
#'
#' @export

createQ <- function(qfile, ...) {
        qdb <- mdb_env(qfile, lock = TRUE, subdir = TRUE,
                       create = TRUE, ...)
        invisible(qdb)
}


#' Initialize a queue
#'
#' Initialize an existing queue created by \code{createQ}
#'
#'
#'
#'
#'
#' @export

initQ <- function(qfile, ...) {
        qdb <- mdb_env(qfile, lock = TRUE, subdir = TRUE,
                       create = FALSE, ...)
        qdb
}


push <- function(qdb, val) {

}


pop <- function(qdb) {

}

is_empty <- function(qdb) {

}

top <- function(qdb) {

}















