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
        expect_true(any_shelf(x) > 0L)
        expect_true(is.character(k$key))
        expect_equal(substr(k$key, 1, 6), "shelf_")
        expect_error(dequeue(x))
        result <- "Result"
        shelf2output(x, k$key, result)
        expect_false(is_empty_output(x))
        val <- dequeue(x)
        expect_equal(val, result)
        expect_true(is_empty_output(x))
        k <- input2shelf(x)
        expect_true(any_shelf(x) > 0L)
        expect_true(is_empty_input(x))
        shelf2output(x, k$key, result)
        expect_false(any_shelf(x) > 0L)
        expect_equal(length(shelf_list(x)), 0L)
        val <- dequeue(x)
        expect_equal(val, result)
        expect_true(is_empty_output(x))
        delete_queue(x)
})

test_that("job_queue loop", {
        x <- list(jq = create_job_queue("test2_jobqueue"))
        n <- 200
        for(i in 1:n) {
                jq <- x$jq
                enqueue(jq, i)
        }
        for(i in 1:n) {
                jq <- x$jq
                k <- input2shelf(jq)
                result <- runif(1)
                shelf2output(jq, k$key, result)
        }
        delete_queue(x$jq)
})


test_that("job_queue in a list", {
        obj <- list(queue = create_job_queue("test3"))
        x <- obj$queue
        expect_equal(class(x), "job_queue")
        enqueue(x, 1)
        expect_equal(peek(x), 1)
        k <- input2shelf(x)
        shelf2output(x, k$key, "hello")
        expect_equal(dequeue(x), "hello")
        delete_queue(x)
})


test_that("shelf2input", {
        x <- create_job_queue("test4")
        n <- 200
        for(i in 1:n) {
                enqueue(x, i)
        }
        for(i in 1:n) {
                input2shelf(x)
        }
        expect_true(is_empty_input(x))
        expect_true(any_shelf(x) > 0L)
        shelf2input(x)
        expect_false(is_empty_input(x))
        peek(x)
        input2shelf(x)
        input2shelf(x)
        expect_false(is_empty_input(x))
        expect_true(is_empty_output(x))
        delete_queue(x)
})



















