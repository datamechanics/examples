import databricks.koalas as ks
from pyspark.sql import SparkSession



if __name__ == "__main__":

    # build spark session
    spark = SparkSession.builder.appName("KoalasPostgresDemo").getOrCreate()

    # build koalas DF
    kdf = ks.DataFrame({"key": [ 1,2], "label": ["foo","bar"]})
    # edit koalas DF in place
    kdf.key.loc[0] = 1000
    
    # convert koalas DataFrame to spark DataFrame
    df = kdf.to_spark()

    # SQL metadata
    properties = {"user": "postgres","postgres": "password","driver": "org.postgresql.Driver"}
    url = "jdbc:postgresql://localhost:5432/postgres"

    # write to db
    df.write.jdbc(url=url, table="koalas", mode="overwrite", properties=properties)





