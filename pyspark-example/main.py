from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, col, sum
from pyspark.sql import SparkSession
import databricks.koalas as ks


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
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    # bucket path
    data_path = "s3a://dataforgood-fb-data/csv/"

    # read data from publc bucket into Spark DF
    df = spark.read.csv(data_path)

    # apply spark transformations
    transformedDF = df.transform(apply_transforms)

    # build Koalas DF from Spark DF
    kDF = ks.DataFrame(transformedDF)
    
    medianDF = kDF.median("population")
    
    # convert koalas DataFrame to spark DataFrame
    finalDF = medianDF.to_spark()

    # SQL metadata
    properties = {"user": "postgres","postgres": "password","driver": "org.postgresql.Driver"}
    url = "jdbc:postgresql://<postgreshost>:5432/postgres"

    # write to db
    finalDF.write.jdbc(url=url, table="koalas", mode="overwrite", properties=properties)





