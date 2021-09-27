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
library(lemon)
library(nycflights13)
library(Lahman)

#data(iris)
#summary(iris)

Sys.setenv(SPARK_HOME = "/opt/spark")

conf <- spark_config()
conf$sparklyr.defaultPackages <- c(
  "com.amazonaws:aws-java-sdk-pom:1.11.828"
)

conf$spark.home <- "/opt/spark"

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
    
    #song_parquet_tbl <- spark_read_parquet(sc, name = "song_parquet", path = "s3a://dm-demo-data-temp/data/partitonYearTuning5.parquet/year=1974")
    #spark_tbl_handle <- spark_read_parquet(sc, "tbl_name_in_spark", "s3a://dm-demo-data-temp/data/partitonYearTuning5.parquet/year=1974")
    #regular_df <- collect(spark_tbl_handle)
    #summary(regular_df)
    iris_tbl <- copy_to(sc, iris, "iris", overwrite = TRUE)
    flights_tbl <- copy_to(sc, nycflights13::flights, "flights", overwrite = TRUE)
    batting_tbl <- copy_to(sc, Lahman::Batting, "batting", overwrite = TRUE)

    #Data Engineering
    #dplyr
    delay <- flights_tbl %>%
      group_by(tailnum) %>%
      summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
      filter(count > 20, dist < 2000, !is.na(delay)) %>%
      collect

    delay

    #spark_write_parquet(flights_tbl, "s3a://dm-demo-data-temp/data/flights")
    spark_write_parquet(flights_tbl, mode="overwrite", "wasbs://deltatest@dmdemodata.blob.core.windows.net/flights")
    spark_write_parquet(batting_tbl, mode="overwrite", "wasbs://deltatest@dmdemodata.blob.core.windows.net/batting")
    spark_write_parquet(iris_tbl, mode="overwrite", "wasbs://deltatest@dmdemodata.blob.core.windows.net/iris")


    writeLines("=========================== Reading parquet table ======================")
    flights_spark_tbl_handle <- spark_read_parquet(sc, "flights_tbl_in_spark", "wasbs://deltatest@dmdemodata.blob.core.windows.net/batting")
    r_df <- collect(flights_spark_tbl_handle)
    r_df
    
    #tbl_cache(sc, "song_parquet")
    #song_parquet_tbl2 <- spark_read_parquet(sc, name = "song_parquet2", path = "s3a://dm-demo-data-temp/data/partitonYearTuning5.parquet/year=1990")
    
    #tbl_cache(sc, "song_parquet2")
    #song_parquet_tbl
    #df3 <- collect(df2)
    #filter(df2, year == 1980)
    #writeLines("=========================== nb rows ===================")
    #nrow(df3)
    #writeLines("=========================== head ======================")
    #head(df3)
    src_tbls(sc) 
    #SQL
    writeLines("=========================== SQL ======================")
    library(DBI)
    spark_tbl_handle_song_preview <- dbGetQuery(sc, "SELECT * FROM tbl_name_in_spark LIMIT 10")
    #regular_song_preview <- collect(spark_tbl_handle_song_preview)
    #summary(regular_song_preview)
    #head(song_preview)
    writeLines("=========================== Collect ======================")
    #song_preview_c <- collect(song_preview)
    #head(song_preview_c)
    #spark_apply 

    #Sys.sleep(60)
    writeLines("============================ End Spark Session ==========================")
    print("[R] Waiting 30 seconds...")
    Sys.sleep(30)
    spark_disconnect(sc)
} else {
    writeLines("Unable to establish a Spark connection!")
}
