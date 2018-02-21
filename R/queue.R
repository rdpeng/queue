#' Create a queue
#'
#' Create a new queue along with necessary files
#'
#' @param qfile the name of the queue
#' @param ... other arguments to be passed to \code{mdb_env}
#'
#' @details The queue will be created as a subdirectory named \code{qfile}
#' under the current working directory
#'
#' @return an object of class \code{"queue"} (invisibly)
#'fet
#' @importFrom thor mdb_env
#' @export

create_queue <- function(qfile, ...) {
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
        invisible(structure(list(queue = qdb,
                                 path = qfile),
                            class = "queue"))
}


#' Initialize a queue
#'
#' Initialize an existing queue created by \code{create_queue}
#'
#' @param qfile the name of the queue
#' @param ... other arguments to be passed to \code{mdb_env}
#'
#' @details \code{qfile} should be a subdirectory under the current working
#' directory
#'
#' @return an object of class \code{"queue"}
#'
#' @importFrom thor mdb_env
#' @export

init_queue <- function(qfile, ...) {
        qdb <- mdb_env(qfile, lock = TRUE, subdir = TRUE,
                       create = FALSE, ...)
        structure(list(queue = qdb, path = qfile),
                  class = "queue")
}

#' @export
print.queue <- function(x, ...) {
        cat(sprintf("<queue: %s>\n", basename(x$path)))
}


#' Add an Element
#'
#' Add a new element on to the back of the queue
#'
#' @param x a queue object
#' @param val an R object
#' @param ... arguments passed to other methods
#'
#' @details Insert an R object into the queue at the tail
#'
#' @return Nothing useful
#'
#' @export
#'
enqueue <- function(x, val, ...) {
        UseMethod("enqueue")
}

#' @export
enqueue.queue <- function(x, val, ...) {
        qdb <- x$queue
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

#' Get the Next Queue Element
#'
#' Return the next queue element and remove it from the queue
#'
#' @param x a queue object
#' @param ... arguments passed to other methods
#'
#' @details Return the head of the queue while also removing the element from
#' the queue
#'
#' @return An R object representing the head of the queue
#'
#' @export
#'
dequeue <- function(x, ...) {
        UseMethod("dequeue")
}


#' @export
dequeue.queue <- function(x, ...) {
        qdb <- x$queue
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
#' @param x a queue object
#' @param ... arguments passed to other methods
#'
#' @return \code{TRUE} or \code{FALSE} depending on whether the queue is empty
#' or not
#'
#' @export
#'
is_empty <- function(x, ...) {
        UseMethod("is_empty")
}

#' @export
is_empty.queue <- function(x, ...) {
        qdb <- x$queue
        val <- fetch(qdb, "head")
        is.null(val)
}

is_empty.mdb_txn <- function(x, ...) {
        val <- fetch(x, "head")
        is.null(val)
}


#' Get the next of the queue
#'
#' Return the next element of the queue
#'
#' @param x a queue object
#' @param ... arguments passed to other methods
#'
#' @return the value of the head of the queue
#'
#' @export
#'
peek <- function(x, ...) {
        UseMethod("peek")
}

#' @export
peek.queue <- function(x, ...) {
        qdb <- x$queue
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






