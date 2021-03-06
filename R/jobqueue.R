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
        txn.f <- function(txn) {
                insert(txn, "in_head", NULL)
                insert(txn, "in_tail", NULL)
                insert(txn, "out_head", NULL)
                insert(txn, "out_tail", NULL)
        }
        qdb$with_transaction(txn.f, write = TRUE)
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
        force(val)
        qdb <- x$queue
        node <- list(value = val,
                     nextkey = NULL,
                     salt = runif(1))
        key <- hash(node)
        txn.f <- function(txn) {
                if(is_empty_input(txn))
                        insert(txn, "in_head", key)
                else {
                        ## Convert tail node to regular node
                        tailkey <- fetch(txn, "in_tail")
                        oldtail <- fetch(txn, tailkey)
                        oldtail$nextkey <- key
                        insert(txn, tailkey, oldtail)
                }
                ## Insert new node and point tail to new node
                insert(txn, key, node)
                insert(txn, "in_tail", key)
        }
        qdb$with_transaction(txn.f, write = TRUE)
        invisible(NULL)
}


#' List elements on the shelf
#'
#' List elements on the shelf
#'
#' @param x a job_queue object
#' @param ... other arguments passed to methods
#'
#' @return a named list of elements on the shelf
#'
#' @export
#'
shelf_list <- function(x, ...) {
        UseMethod("shelf_list")
}


#' @export
shelf_list.job_queue <- function(x, ...) {
        qdb <- x$queue
        txn.f <- function(txn) {
                keys <- txn$list(starts_with = "shelf_")

                if(length(keys) > 0L)
                        vals <- mfetch(txn, keys)
                else
                        vals <- list()
                vals
        }
        qdb$with_transaction(txn.f, write = TRUE)
}


#' Retrieve an element from the shelf
#'
#' Retrieve an element from the shelf given a key
#'
#' @param x a job_queue object
#' @param key a shelf identifier key
#' @param ... other arguments passed to methods
#'
#' @export
#'
shelf_get <- function(x, key, ...) {
        UseMethod("shelf_get")
}

#' @export
shelf_get.job_queue <- function(x, key, ...) {
        qdb <- x$queue
        txn.f <- function(txn) {
                node <- fetch(txn, key)
                node$value
        }
        qdb$with_transaction(txn.f, write = TRUE)
}


#' Move from Input to Shelf
#'
#' Dequeue the input queue and move that element to the shelf
#'
#' @param x a job_queue object
#' @param ... other arguments passed to methods
#'
#' @export
#'
input2shelf <- function(x, ...) {
        UseMethod("input2shelf")
}

#' @export
input2shelf.job_queue <- function(x, ...) {
        qdb <- x$queue
        txn.f <- function(txn) {
                ## Dequeue the input queue
                if(is_empty_input(txn))
                        stop("input queue is empty")
                h <- fetch(txn, "in_head")
                node <- fetch(txn, h)
                insert(txn, "in_head", node$nextkey)
                delete(txn, h)
                val <- node$value

                ## Insert into the shelf
                shelf_node <- list(value = val,
                                   salt = runif(1))

                ## Special shelf identifier prefix
                key <- paste0("shelf_", hash(shelf_node))
                insert(txn, key, shelf_node)
                list(key = key, value = val)
        }
        qdb$with_transaction(txn.f, write = TRUE)
}


#' Move from Shelf to Output Queue
#'
#' Move an object from the shelf to the output queue
#'
#' @param x a job_queue object
#' @param key identifier for shelf object
#' @param val an R object to be put in the output queue
#' @param ... other arguments passed to methods
#'
#' @export
#'
shelf2output <- function(x, key, val, ...) {
        UseMethod("shelf2output")
}

#' @export
shelf2output.job_queue <- function(x, key, val, ...) {
        qdb <- x$queue
        txn.f <- function(txn) {
                ## Delete from shelf
                delete(txn, key)

                ## Add val to output queue
                node <- list(value = val,
                             nextkey = NULL,
                             salt = runif(1))
                key <- hash(node)

                if(is_empty_output(txn))
                        insert(txn, "out_head", key)
                else {
                        ## Convert tail node to regular node
                        tailkey <- fetch(txn, "out_tail")
                        oldtail <- fetch(txn, tailkey)
                        oldtail$nextkey <- key
                        insert(txn, tailkey, oldtail)
                }
                ## Insert new node and point tail to new node
                insert(txn, key, node)
                insert(txn, "out_tail", key)
        }
        qdb$with_transaction(txn.f, write = TRUE)
        invisible(NULL)
}


#' @export
dequeue.job_queue <- function(x, ...) {
        qdb <- x$queue
        txn.f <- function(txn) {
                if(is_empty_output(txn))
                        stop("output queue is empty")
                h <- fetch(txn, "out_head")
                node <- fetch(txn, h)
                insert(txn, "out_head", node$nextkey)
                delete(txn, h)
                node$value
        }
        val <- qdb$with_transaction(txn.f, write = TRUE)
        val
}


#' @export
peek.job_queue <- function(x, ...) {
        qdb <- x$queue
        txn.f <- function(txn) {
                key <- fetch(txn, "in_head")
                node <- fetch(txn, key)
                node$value
        }
        tryCatch({
                qdb$with_transaction(txn.f, write = FALSE)
        }, error = function(e) {
                stop("problem retrieving head value; queue is likely empty",
                     call. = FALSE)
        })
}

#' @export
delete_queue.job_queue <- function(x, ...) {
        x$queue$destroy()
}

#' Check if Input Queue is Empty
#'
#' Check to see if the input queue is empty
#'
#' @param x a job_queue object
#' @param ... arguments passed to other methods
#'
#' @return \code{TRUE} or \code{FALSE} depending on whether the input queue
#' is empty or not
#'
#' @export
#'
is_empty_input <- function(x, ...) {
        UseMethod("is_empty_input")
}

#' @export
is_empty_input.job_queue <- function(x, ...) {
        qdb <- x$queue
        val <- fetch(qdb, "in_head")
        is.null(val)
}

is_empty_input.mdb_txn <- function(x, ...) {
        val <- fetch(x, "in_head")
        is.null(val)
}


#' Check if Output Queue is Empty
#'
#' Check to see if the output queue is empty
#'
#' @param x a job_queue object
#' @param ... arguments passed to other methods
#'
#' @return \code{TRUE} or \code{FALSE} depending on whether the output queue
#' is empty or not
#'
#' @export
#'
is_empty_output <- function(x, ...) {
        UseMethod("is_empty_output")
}

#' @export
is_empty_output.job_queue <- function(x, ...) {
        qdb <- x$queue
        val <- fetch(qdb, "out_head")
        is.null(val)
}

is_empty_output.mdb_txn <- function(x, ...) {
        val <- fetch(x, "out_head")
        is.null(val)
}



#' Check Shelf for Objects
#'
#' Are there any objects on the shelf?
#'
#' @param x a job_queue object
#' @param ... arguments passed to methods
#'
#' @return the number of items on the shelf
#' @export
#'
any_shelf <- function(x, ...) {
        UseMethod("any_shelf")
}

#' @export
any_shelf.job_queue <- function(x, ...) {
        keys <- x$queue$list(starts_with = "shelf_")
        length(keys)
}




#' Move from Shelf to Input Queue
#'
#' Move all objects from the shelf to the input queue
#'
#' @param x a job_queue object
#' @param ... other arguments passed to methods
#'
#' @export
#'
shelf2input <- function(x, ...) {
        UseMethod("shelf2input")
}


#' @export
shelf2input.job_queue <- function(x, ...) {
        qdb <- x$queue
        txn.f <- function(txn) {
                keys <- txn$list(starts_with = "shelf_")

                if(!length(keys))
                        stop("there are no items on the shelf")

                for(skey in keys) {
                        val <- fetch(txn, skey)

                        ## Delete from shelf
                        delete(txn, skey)

                        ## Add val to input queue
                        node <- list(value = val$value,
                                     nextkey = NULL,
                                     salt = runif(1))
                        key <- hash(node)

                        if(is_empty_input(txn))
                                insert(txn, "in_head", key)
                        else {
                                ## Convert tail node to regular node
                                tailkey <- fetch(txn, "in_tail")
                                oldtail <- fetch(txn, tailkey)
                                oldtail$nextkey <- key
                                insert(txn, tailkey, oldtail)
                        }
                        ## Insert new node and point tail to new node
                        insert(txn, key, node)
                        insert(txn, "in_tail", key)
                }
        }
        qdb$with_transaction(txn.f, write = TRUE)
        invisible(NULL)
}







