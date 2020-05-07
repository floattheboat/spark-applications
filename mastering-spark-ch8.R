






letters <- data.frame(x = letters, y = 1:length(letters))

dir.create("data-csv")
write.csv(letters[1:3, ], "data-csv/letters1.csv", row.names = FALSE)
write.csv(letters[1:3, ], "data-csv/letters2.csv", row.names = FALSE)

do.call("rbind", lapply(dir("data-csv", full.names = TRUE), read.csv))




spark_read_csv(sc, "data-csv/")



spec_with_r <- sapply(read.csv("data-csv/letters1.csv", nrows = 10), class)
spec_with_r



spec_explicit <- c(x = "character", y = "numeric")
spec_explicit



spark_read_csv(sc, "data-csv/", columns = spec_with_r)



spec_compatible <- c(my_letter = "character", my_number = "character")

spark_read_csv(sc, "data-csv/", columns = spec_compatible)


mapped_csv <- spark_read_csv(sc, "data-csv/", memory = FALSE)



# when not all columns are needed on the data read

# use dplyr select and compute with spark as the stage for faster reads

mapped_csv %>%
    dplyr::select(y) %>%
    dplyr::compute("test")



options(sparklyr.sanitize.column.names = FALSE)
copy_to(sc, iris, overwrite = TRUE)



library(bench)
numeric <- copy_to(sc, data.frame(nums = runif(10^4)))
bench::mark(
    CSV = spark_write_csv(numeric, "data.csv", mode = "overwrite"),
    JSON = spark_write_json(numeric, "data.json", mode = "overwrite"),
    Parquet = spark_write_parquet(numeric, "data.parquet", mode = "overwrite"),
    ORC = spark_write_parquet(numeric, "data.orc", mode = "overwrite"),
    iterations = 20
) %>% ggplot2::autoplot()



#### cassandra ####


sc <- spark_connect(master = "local", version = "2.3", config = list(
    sparklyr.connect.packages = "datastax:spark-cassandra-connector:2.3.1-s_2.11"))

spark_read_source(
    sc, 
    name = "emp",
    source = "org.apache.spark.sql.cassandra",
    options = list(keyspace = "dev", table = "emp"),
    memory = FALSE)
















