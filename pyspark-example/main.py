import os, datetime
from databricks import koalas as ks
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, col, sum, lit
from pyspark.sql import SparkSession

def apply_transforms(df: DataFrame) -> DataFrame:

    # split _c0 column as it is a string and we want the population data from it
    split_col = split(df['_c0'], '\t')

    # add population column, group by country, sum population
    return df \
            .withColumn("population", split_col.getItem(2).cast('float')) \
            .groupBy("country") \
            .agg(col("country"), sum("population")).select(col("country"), col("sum(population)") \
            .alias("population"))

if __name__ == "__main__":

    # build spark session
    spark = SparkSession.builder.appName("KoalasPostgresDemo").getOrCreate()

    # Enable hadoop s3a settings
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
    "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    # read data from publc bucket into Spark DF
    data_path = "s3a://dataforgood-fb-data/csv/"
    df = spark.read.csv(data_path)

    # apply spark transformations
    transformedDF = df.transform(apply_transforms)

    # build Koalas DF from Spark DF, get median, convert back to Spark DataFrame, add column with current date
    kDF = ks.DataFrame(transformedDF)
    medianDF = kDF.median().withColumn
    finalDF = medianDF.to_spark().withColumn("date", lit(datetime.datetime.now()))

    # SQL metadata
    properties = {"user": os.environ['PG_USER'],"postgres": os.environ['PG_PASSWORD'],"driver": "org.postgresql.Driver"}
    url = f"jdbc:postgresql://{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB_NAME']}"

    # write to db
    finalDF.write.jdbc(url=url, table="koalas", mode="overwrite", properties=properties)





