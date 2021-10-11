# 2021-06-02
# Standard R setup + config required for running on Data Mechainics k8s
# sparklyr: 1.6.3
# jeremy.jacobs@ookla.com
# --------------------------------------------------------------------------------------

library(stringr)
library(arrow)
library(sparklyr)
library(dplyr)
library(datasets)

data(iris)
summary(iris)

#install.packages(c("nycflights13", "Lahman"))

Sys.setenv(SPARK_HOME = "/opt/spark")

# Config --------------------------------------------------------------------------------
k8_host <- Sys.getenv("KUBERNETES_SERVICE_HOST")
k8_port <- Sys.getenv("KUBERNETES_PORT_443_TCP_PORT")

conf <- spark_config()
conf$sparklyr.defaultPackages <- c(
  "com.amazonaws:aws-java-sdk-pom:1.11.828"
)

conf$spark.home <- "/opt/spark"
conf$spark.master <- str_glue("k8s://https://{k8_host}:{k8_port}")

conf$sparklyr.arrow <- TRUE
conf$sparklyr.apply.packages <- FALSE
conf$sparklyr.gateway.routing <- FALSE
conf$`sparklyr.shell.deploy-mode` <- "client"
conf$spark.kubernetes.file.upload.path <- "file:///tmp"
conf$sparklyr.connect.app.jar <- "local:///usr/local/lib/R/site-library/sparklyr/java/sparklyr-master-2.12.jar"

# Modified from an issue comment in the Sparklyr repo
conf$sparklyr.connect.aftersubmit <- function() {
  # wait for pods to launch
  print("[R] Waiting 30 seconds...")
  Sys.sleep(30)
  # configure port forwarding
  system2(
    "kubectl",
    c("port-forward", "driver-r", "8880:8880", "8881:8881", "4040:4040"),
    wait = FALSE
  )
}

sc <- spark_connect(config = conf, spark_home = "/opt/spark", scala_version = "2.12.11")


# Access executor env variable
#
# Set via spark config:
# "sparkConf": {
#     "spark.executorEnv.SOME_VAR": "01234"
# }

runtime_config <- spark_context_config(sc)
some_var <- runtime_config$spark.executorEnv.SOME_VAR


# Basic R Spark jobs with no dependencies ----------------------------------------------
packageVersion("sparklyr")

if (!is.null(sc)) {
    iris_tbl <- copy_to(sc, iris, overwrite = TRUE)
    #flights_tbl <- copy_to(sc, nycflights13::flights, "flights", overwrite = TRUE)
    #batting_tbl <- copy_to(sc, Lahman::Batting, "batting", overwrite = TRUE)
    src_tbls(sc)

    a <- collect(iris_tbl)
    summary(a)


    writeLines("=========================== SQL ======================")
    #library(DBI)
    #iris_preview_tbl <- dbGetQuery(sc, "SELECT * FROM iris_spark LIMIT 10")
    #iris_preview <- collect(iris_tbl)
    #data(iris_preview)
    #summary(iris_preview)

    writeLines("=========================== Reading parquet table ======================")
    #temp_csv <- tempfile(fileext = ".csv")
    temp_parquet <- tempfile(fileext = ".parquet")
    #temp_json <- tempfile(fileext = ".json")

    #spark_write_csv(iris_tbl, temp_csv)
    #iris_csv_tbl <- spark_read_csv(sc, "iris_csv", temp_csv)

    spark_write_parquet(iris_tbl, temp_parquet)
    iris_parquet_tbl <- spark_read_parquet(sc, "iris_parquet", temp_parquet)
    

    #spark_write_parquet(iris_parquet_tbl, "s3a://dm-demo-data-temp/data/iris")


    writeLines("=========================== Reading Writing JSON ========================")
    #spark_write_json(iris_tbl, temp_json)
    #iris_json_tbl <- spark_read_json(sc, "iris_json", temp_json)

    writeLines("============================ End Spark Session ==========================")
    spark_disconnect(sc)
} else {
    writeLines("Unable to establish a Spark connection!")
}
