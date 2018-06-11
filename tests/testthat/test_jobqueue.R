test_that("job_queue", {
        x <- create_job_queue("test_jobqueue")
        expect_equal(class(x), "job_queue")
        enqueue(x, 1)
        enqueue(x, 2)
        expect_equal(peek(x), 1)
        expect_false(is_empty_input(x))
        expect_true(is_empty_output(x))
        expect_error(dequeue(x))
        k <- input2shelf(x)
        expect_true(is.character(k))
        expect_equal(substr(k, 1, 6), "shelf_")
        expect_error(dequeue(x))
        result <- "Result"
        shelf2output(x, k, result)
        expect_false(is_empty_output(x))
        val <- dequeue(x)
        expect_equal(val, result)
        expect_true(is_empty_output(x))
        k <- input2shelf(x)
        expect_true(is_empty_input(x))
        shelf2output(x, k, result)
        expect_equal(length(shelf_list(x)), 0L)
        val <- dequeue(x)
        expect_equal(val, result)
        expect_true(is_empty_output(x))
        unlink("test_jobqueue", recursive = TRUE)
})

test_that("job_queue loop", {
        x <- create_job_queue("test2_jobqueue")
        n <- 200
        for(i in 1:n) {
                enqueue(x, i)
        }
        keys <- integer(n)
        for(i in 1:n) {
                keys[i] <- input2shelf(x)
        }
        for(i in 1:n) {
                result <- runif(1)
                shelf2output(x, keys[i], result)
        }
        output <- numeric(n)
        for(i in 1:n) {
                output[i] <- dequeue(x)
        }
        unlink("test2_jobqueue", recursive = TRUE)
})