---
output: github_document
---

<!-- README.md is generated from README.Rmd. Please edit that file -->



# queue

The goal of queue is to provide a simple on-disk queue data structure in R.

## Installation

You can install queue from github with:


```r
# install.packages("remotes")
remotes::install_github("rdpeng/queue")
```

## Example

This is a basic example which shows you how to solve a common problem:


```r
library(queue)
x <- create_queue("myqueue")
is_empty(x)
#> [1] TRUE

## Queue up some items
enqueue(x, 1)
enqueue(x, 2)

## Look at the head of the queue
peek(x)
#> [1] 1

## Retrieve some items
dequeue(x)
#> [1] 1
dequeue(x)
#> [1] 2

is_empty(x)
#> [1] TRUE
```
