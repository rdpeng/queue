## Data structure for parallel job queue
##
## Basic elements:
## 1. Input task queue
## 2. Shelf where tasks placed during work
## 3. Output queue when tasks are finished


#' Create a Job Queue
#'
#' Create a job queue and return a job queue object
#'
#' @param qfile the name of the queue
#' @param mapsize size of the map for LMDB database
#' @param ... other arguments to be passed to \code{mdb_env}
#'
#'
#' @details The queue will be created as a subdirectory named \code{qfile}
#' under the current working directory
#'
#' @return an object of class \code{"job_queue"} (invisibly)
#'fet
#' @importFrom thor mdb_env
#' @export
#'
create_job_queue <- function(qfile, mapsize = 2^30, ...) {
        qdb <- mdb_env(qfile, lock = TRUE, subdir = TRUE,
                       create = TRUE, mapsize = mapsize, ...)
        txn <- qdb$begin(write = TRUE)
        tryCatch({
                insert(txn, "in_head", NULL)
                insert(txn, "in_tail", NULL)
                insert(txn, "out_head", NULL)
                insert(txn, "out_tail", NULL)
                txn$commit()
        }, error = function(e) {
                txn$abort()
                stop(e)
        })
        invisible(structure(list(queue = qdb,
                                 path = qfile),
                            class = "job_queue"))
}


#' Initialize a Job Queue
#'
#' Initialize an existing job queue created by \code{create_job_queue}
#'
#' @param qfile the name of the job queue
#' @param mapsize size of the map for LMDB database
#' @param ... other arguments to be passed to \code{mdb_env}
#'
#' @details \code{qfile} should be a subdirectory under the current working
#' directory
#'
#' @return an object of class \code{"job_queue"}
#'
#' @importFrom thor mdb_env
#' @export
#'
init_job_queue <- function(qfile, mapsize = 2^30, ...) {
        qdb <- mdb_env(qfile, lock = TRUE, subdir = TRUE,
                       create = FALSE, mapsize = mapsize, ...)
        structure(list(queue = qdb, path = qfile),
                  class = "job_queue")
}

#' @export
print.job_queue <- function(x, ...) {
        cat(sprintf("<job_queue: %s>\n", basename(x$path)))
}


#' @export
enqueue.job_queue <- function(x, val, ...) {

}

#' Move from Input to Shelf
#'
#' Dequeue the input queue and move that element to the shelf
#'
#' @param x a job_queue object
#'
#' @export
#'
input2shelf <- function(x, ...) {
        UseMethod("dequeue2shelf")
}


#' @export
input2shelf.job_queue <- function(x, ...) {

}

#' Move from Shelf to Output Queue
#'
#' Move an object from the shelf to the output queue
#'
#' @param x a job_queue object
#' @param key identifier for shelf object
#'
#' @export
#'
shelf2output <- function(x, key, ...) {

}


#' @export
shelf2output.job_queue <- function(x, key, ...) {

}


#' @export
dequeue.job_queue <- function(x, ...) {

}






















