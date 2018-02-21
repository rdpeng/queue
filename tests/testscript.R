## Test queue

library(queue)
dbname <- tempfile()
db <- create_queue(dbname)
library(thor)
db2 <- mdb_env(dbname)

library(queue)
dbname <- tempfile()
qdb <- create_queue(dbname)
is_empty(qdb)
enqueue(qdb, 1)

library(thor)
dbname <- tempfile()
e <- mdb_env(dbname)
txn <- e$begin(write = TRUE)
queue:::insert(txn, "a", 1)
txn$commit()
e$list()
unserialize(e$get("a"))

enqueue(qdb, 2)
enqueue(qdb, 3)
is_empty(qdb)
peek(qdb)
dequeue(qdb)
peek(qdb)
dequeue(qdb)

library(queue)
dbname <- tempfile()
print(dbname)
qdb <- create_queue(dbname)
enqueue(qdb, 1)


