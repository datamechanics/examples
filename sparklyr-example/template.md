
# Sparklyr config template

---


```json
{
  "mainClass": "sparklyr.Shell",
  "mainApplicationFile": "local:///usr/local/lib/R/site-library/sparklyr/java/sparklyr-3.0-2.12.jar",
  "sparkConf": {
    "sparklyr.arrow": "TRUE",
    "spark.r.command": "/usr/bin/Rscript",
    "spark.r.libpaths": "/usr/lib/R/library",
    "spark.executorEnv.SOME_VAR": "01234",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "0"
  },
  "hadoopConf": {
    "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "fs.s3a.endpoint": "s3.us-west-2.amazonaws.com",
    "fs.s3a.assumed.role.arn": "arn:aws:iam::202306134469:role/datamechanics-tf",
    "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
    "fs.s3a.assumed.role.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,com.amazonaws.auth.EnvironmentVariableCredentialsProvider,com.amazonaws.auth.InstanceProfileCredentialsProvider"
  }
}
```

---

## Notes

**sparklyr.arrow:** The driver will complain about this setting and it still needs to be tested.

```
WARN SQLConf: The SQL config 'spark.sql.execution.arrow.enabled' has been deprecated in Spark v3.0 and may be removed in the future. Use 'spark.sql.execution.arrow.pyspark.enabled' instead of it.
```

**spark.sql.execution.arrow.maxRecordsPerBatch=0:**

This is required when using `spark_apply` which executes a function on a partition of data (e.g. mean).
`sdf_repartition` is also required to force group calculation.
