

library(sparklyr)
library(DBI)
library(dplyr)
library(sparklyr.nested)
library(corrr)
library(dbplot)

sc <- spark_connect(master = "local")


#### chapter 2 ####

# read dataset into spark

# cars <- copy_to(sc, mtcars)
# 
# 
# dbGetQuery(sc, "SELECT count(*) FROM mtcars")
# 
# 
# count(cars)
# 
# select(cars, hp, mpg) %>% 
#     sample_n(100) %>% 
#     collect() %>% 
#     plot()
# 
# 
# model <- ml_linear_regression(cars, mpg ~ hp)
# model
# 
# 
# model %>% 
#     ml_predict(copy_to(sc, data.frame(hp = 250 + 10 * 1:10))) %>% 
#     transmute(hp = hp, mpg = prediction) %>% 
#     full_join(select(cars, hp, mpg)) %>% 
#     collect() %>% 
#     plot()
# 
# 
# 
# 
# sparklyr.nested::sdf_nest(cars, hp) %>% 
#     group_by(cyl) %>% 
#     summarise(data = collect_list(data))
# 
# 
# 
# # streaming
# 
# dir.create("input")
# write.csv(mtcars, "input/cars_1.csv", row.names = F)
# 
# stream <- stream_read_csv(sc, "input/") %>% 
#     select(mpg, cyl, disp) %>% 
#     stream_write_csv("output/")
# 
# write.csv(mtcars, "input/cars_2.csv", row.names = F)
# 
# stream_stop(stream)
# 
# spark_log(sc)
# 
# spark_log(sc, filter = "sparklyr")




#### chapter 3 ####

cars <- copy_to(sc, mtcars)

summarise_all(cars, mean)


summarise_all(cars, mean) %>% 
    show_query()

# percentile using Hive

summarise(cars, mpg_percentile = percentile(mpg, 0.25))

summarise(cars, mpg_percentile = percentile(mpg, 0.25)) %>% 
    show_query()


# array in Hive

summarise(cars, mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75)))

summarise(cars, mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75))) %>% 
    mutate(mpg_percentile = explode(mpg_percentile))



# correlations

ml_corr(cars)

correlate(cars, use = "pairwise.complete.obs", method = "pearson")


correlate(cars, use = "pairwise.complete.obs", method = "pearson") %>% 
    shave() %>% 
    rplot()




# visualization using R/ggplot2

# car_group <- cars %>% 
#     group_by(cyl) %>% 
#     summarise(mpg = sum(mpg, na.rm = TRUE)) %>% 
#     collect() %>% 
#     print()



# visualization using spark/dbplot

cars %>% 
    dbplot_histogram(mpg, binwidth = 3) +
    labs(title = "MPG Distribution",
         subtitle = "Histogram over miles per gallon")


dbplot_raster(cars, mpg, wt, resolution = 16)




# modeling

cars %>% 
    ml_linear_regression(mpg ~ .) %>% 
    summary()



# caching

cached_cars <- cars %>% 
    mutate(cyl = paste0("cyl_", cyl)) %>% 
    compute("cached_cars")

cached_cars %>% 
    ml_linear_regression(mpg ~ .) %>% 
    summary()












