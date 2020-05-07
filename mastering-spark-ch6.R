








# Retrieve the Spark installation directory
spark_home <- spark_home_dir()

# Build paths and classes
spark_path <- file.path(spark_home, "bin", "spark-class")

# Start cluster manager master node
system2(spark_path, "org.apache.spark.deploy.master.Master", wait = FALSE)






system2(spark_path, c("org.apache.spark.deploy.worker.Worker",
                      "spark://address:port"), wait = FALSE)










