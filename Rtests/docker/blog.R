# 2021-06-02
# Standard R setup + config required for running on Data Mechainics k8s
# sparklyr: 1.6.3
# jeremy.jacobs@ookla.com
# --------------------------------------------------------------------------------------

#library(stringr)
#library(arrow)
#library(sparklyr)
#library(dplyr)
#library(datasets)
#library(lemon)


#knit_print.data.frame <- lemon_print

library(stringr)
library(arrow)
library(sparklyr)
library(dplyr)
################################
# Create the Spark Session
################################

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
packageVersion("sparklyr") #sparklyr version

################################
# Process Data with Spark
################################

if (!is.null(sc)) {
    
    # Load Data
    # copy_to create a spark Dataframe
    writeLines("=========================== Create Spark Data frame ======================")
    library(nycflights13)
    library(Lahman)
    flights_tbl_spark <- copy_to(sc, nycflights13::flights, "flights", overwrite = TRUE)
    iris_tbl_spark <- copy_to(sc, iris, "iris", overwrite = TRUE)
    batting_tbl_spark <- copy_to(sc, Lahman::Batting, "batting", overwrite = TRUE)

    print(src_tbls(sc))

    writeLines("=========================== flights table ======================")
    print(nycflights13::flights)
    #Data Engineering
    #dplyr
    writeLines("=========================== flights delay ======================")
    delay_tbl_spark <- flights_tbl_spark %>%
      group_by(tailnum) %>%
      summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
      filter(count > 20, dist < 2000, !is.na(delay)) 

    delay_r = collect(delay_tbl_spark)
    print(delay_r)

    #spark_write_parquet(flights_tbl, "s3a://dm-demo-data-temp/data/flights")
    writeLines("=========================== Writing parquet ======================")
    spark_write_parquet(flights_tbl_spark, mode="overwrite", "wasbs://deltatest@dmdemodata.blob.core.windows.net/flights")
    spark_write_parquet(batting_tbl_spark, mode="overwrite", "wasbs://deltatest@dmdemodata.blob.core.windows.net/batting")
    spark_write_parquet(iris_tbl_spark, mode="overwrite", "wasbs://deltatest@dmdemodata.blob.core.windows.net/iris")


    writeLines("=========================== Reading parquet  ======================")
    flights_tbl_spark2 <- spark_read_parquet(sc, "flights_tbl_in_spark", "wasbs://deltatest@dmdemodata.blob.core.windows.net/flights")
    flights_r2 <- collect(flights_tbl_spark2)
    print(flights_r2)
    
    writeLines("=========================== Caching Table  ======================")
    tbl_cache(sc, "flights")

    writeLines("=========================== SQL Queries  ======================")
    library(DBI)
    flight_preview_spark_tbl <- dbGetQuery(sc, "SELECT * FROM flights LIMIT 5")
    flights_preview_r = collect(flight_preview_spark_tbl)
    print(flights_preview_r)
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
    

    #regular_song_preview <- collect(spark_tbl_handle_song_preview)
    #summary(regular_song_preview)
    #head(song_preview)
    writeLines("=========================== spark_apply ======================")
    iris_tbl_spark2 <- spark_apply(iris_tbl_spark, function(data) {
        data[2:4] + rgamma(1,2)
    })
    iris_r = collect(iris_tbl_spark2)
    print(iris_r)



  # plot delays
  writeLines("=========================== Plots ======================")

      #Plots
  delay <- flights_tbl_spark %>%
        group_by(tailnum) %>%
        summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
        filter(count > 20, dist < 2000, !is.na(delay)) %>%
        collect

  library(ggplot2)
  plot1 <- ggplot(delay, aes(dist, delay)) +
    geom_point(aes(size = count), alpha = 1/2) +
    geom_smooth() +
    scale_size_area(max_size = 2)

  pdf("plot.pdf")
  print(plot1)
  dev.off()

    #Sys.sleep(60)
    writeLines("============================ End Spark Session ==========================")
    print("[R] Waiting 30 seconds...")
    Sys.sleep(120)
    spark_disconnect(sc)
} else {
    writeLines("Unable to establish a Spark connection!")
}
