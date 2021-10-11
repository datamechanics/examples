# 2021-06-02
# Standard R setup + config required for running on Data Mechainics k8s
# sparklyr: 1.6.3
# jeremy.jacobs@ookla.com
# --------------------------------------------------------------------------------------

library(stringr)
library(arrow)
library(sparklyr)

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
conf$sparklyr.connect.app.jar <- "local:///usr/local/lib/R/site-library/sparklyr/java/sparklyr-3.0-2.12.jar"

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


# Basic spark app ----------------------------------------------------------------------
# Modified from https://github.com/yitao-li/sparklyr-pyspark-benchmarks/blob/master/sparklyr_spark_apply_benchmark/sparklyr_spark_apply_benchmark.R

n <- 100

df <- data.frame(lapply(seq(10), function(c) sapply(runif(n = n, min = -2147483648, max = 2147483647), as.integer)))
colnames(df) <- c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J")
sdf <- sdf_copy_to(sc, df, overwrite = TRUE)
tdf <- sdf_collect(sdf)

head(tdf)
spark_disconnect(sc)


# Basic spark_apply --------------------------------------------------------------------
# From section 11.2.2: https://therinspark.com/distributed.html#partitioned-modeling

iris <- copy_to(sc, datasets::iris)

iris %>%
    spark_apply(nrow, group_by = "Species")

iris %>%
    spark_apply(
        function(e) summary(lm(Petal_Length ~ Petal_Width, e))$r.squared,
        names = "r.squared",
        group_by = "Species"
        )

head(iris)
spark_disconnect(sc)
