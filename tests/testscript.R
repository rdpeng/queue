## Test queue

library(queue)

dbname <- tempfile()
queue::create_Q(dbname)
qdb <- init_Q(dbname)
is_empty(qdb)
enqueue(qdb, 1)
enqueue(qdb, 2)
enqueue(qdb, 3)
is_empty(qdb)
peek(qdb)
dequeue(qdb)
peek(qdb)
dequeue(qdb)
