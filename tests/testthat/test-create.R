test_that("create", {
        dbname <- tempfile()
        qdb <- create_queue(dbname)
        expect_is(qdb, "queue")
        expect_true(is_empty(qdb))
        enqueue(qdb, 1)
        expect_false(is_empty(qdb))
})

test_that("operations", {
        dbname <- tempfile()
        qdb <- create_queue(dbname)
        expect_true(is_empty(qdb))
        enqueue(qdb, 2)
        enqueue(qdb, 3)
        expect_false(is_empty(qdb))
        expect_equal(peek(qdb), 2)
        dequeue(qdb)
        expect_equal(peek(qdb), 3)
        dequeue(qdb)
        expect_true(is_empty(qdb))
})

test_that("unique keys", {
        dbname <- tempfile()
        qdb <- create_queue(dbname)
        expect_true(is_empty(qdb))
        enqueue(qdb, 1)
        enqueue(qdb, 2)
        enqueue(qdb, 3)
        enqueue(qdb, 1)
        x <- dequeue(qdb)
        expect_equal(x, 1)
        expect_false(is_empty(qdb))
})


test_that("peek", {
        x <- create_queue("peek_test")
        expect_error(peek(x))
        unlink("peek_test", recursive = TRUE)
})