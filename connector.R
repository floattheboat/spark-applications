



library(sparklyr)

sc <- spark_connect(master = "local")


spark_disconnect(sc)
