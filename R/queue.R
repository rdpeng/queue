#' Create a queue
#'
#' Create a new queue along with necessary files
#'
#' @param qfile the name of the queue
#' @param ... other arguments to be passed to \code{mdb_env}
#'
#' @details The queue will be created as a subdirectory named \code{qfile} under the current working directory
#'
#' @importFrom thor mdb_env
#' @export

create_Q <- function(qfile, ...) {
        qdb <- mdb_env(qfile, lock = TRUE, subdir = TRUE,
                       create = TRUE, ...)
        txn <- qdb$begin(write = TRUE)
        tryCatch({
                insert(txn, "head", NULL)
                insert(txn, "tail", NULL)
                txn$commit()
        }, error = function(e) {
                txn$abort()
                stop(e)
        })
        invisible(qdb)
}


#' Initialize a queue
#'
#' Initialize an existing queue created by \code{createQ}
#'
#' @param qfile the name of the queue
#' @param ... other arguments to be passed to \code{mdb_env}
#'
#' @details \code{qfile} should be a subdirectory under the current working directory
#'
#' @importFrom thor mdb_env
#' @export

init_Q <- function(qfile, ...) {
        qdb <- mdb_env(qfile, lock = TRUE, subdir = TRUE,
                       create = FALSE, ...)
        qdb
}


#' Push an Element
#'
#' Push a new element on to the back of the queue
#'
#' @param qdb a queue object
#' @param val an R object
#'
#' @details Insert an R object into the queue at the tail
#'
#' @return Nothing useful
#'
#' @export
#'
enqueue <- function(qdb, val) {
        node <- list(value = val,
                     nextkey = NULL)
        key <- hash(node)
        txn <- qdb$begin(write = TRUE)
        tryCatch({
                if(is_empty(txn))
                        insert(txn, "head", key)
                else {
                        ## Convert tail node to regular node
                        tailkey <- fetch(txn, "tail")
                        oldtail <- fetch(txn, tailkey)
                        oldtail$nextkey <- key
                        insert(txn, tailkey, oldtail)
                }
                ## Insert new node and point tail to new node
                insert(txn, key, node)
                insert(txn, "tail", key)
                txn$commit()
        }, error = function(e) {
                txn$abort()
                stop(e)
        })
        invisible(NULL)
}

#' Pop the Next Queue Element
#'
#' Return the next queue element and remove it from the queue
#'
#' @param qdb a queue object
#'
#' @details Return the head of the queue while also removing the element from the queue
#'
#' @return An R object representing the head of the queue
#'
#' @export
#'
dequeue <- function(qdb) {
        txn <- qdb$begin(write = TRUE)
        tryCatch({
                if(is_empty(txn))
                        stop("queue is empty")
                h <- fetch(txn, "head")
                node <- fetch(txn, h)
                insert(txn, "head", node$nextkey)
                delete(txn, h)
                val <- node$value
                txn$commit()
        }, error = function(e) {
                txn$abort()
                stop(e)
        })
        val

}

#' Check if Queue is Empty
#'
#' Check to see if the queue is empty
#'
#' @param qdb a queue object
#'
#' @return \code{TRUE} or \code{FALSE} depending on whether the queue is empty or not
#'
#' @export
#'
is_empty <- function(qdb) {
        val <- fetch(qdb, "head")
        is.null(val)
}

#' Get the next of the queue
#'
#' Return the next element of the queue
#'
#' @param qdb a queue object
#'
#' @return the value of the head of the queue
#'
#' @export
#'
peek <- function(qdb) {
        txn <- qdb$begin(write = FALSE)
        tryCatch({
                key <- fetch(txn, "head")
                node <- fetch(txn, key)
                val <- node$value
                txn$commit()
        }, error = function(e) {
                txn$abort()
                message("problem getting top value")
                stop(e)
        })
        val
}




## Helper functions

delete <- function(obj, key) {
        obj$del(key)
}

insert <- function(obj, key, value) {
        value_raw <- serialize(value, NULL)
        obj$put(key, value_raw)
}

fetch <- function(obj, key) {
        value_raw <- obj$get(key)
        unserialize(value_raw)
}

#' @importFrom digest sha1
hash <- function(x) {
        sha1(x, digits = 14L, zapsmall = 7L)
}






