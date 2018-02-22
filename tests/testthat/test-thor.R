test_that("thor", {
        dbname <- tempfile()
        e <- mdb_env(dbname)
        input <- 1
        txn <- e$begin(write = TRUE)
        queue:::insert(txn, "a", input)
        txn$commit()
        expect_equal(e$list(), "a")
        output <- unserialize(e$get("a"))
        expect_identical(output, input)
})

