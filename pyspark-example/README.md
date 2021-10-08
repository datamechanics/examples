This tutorial demonstrates the process of building a Spark Docker image from one of Data Mechanic's [base images](https://hub.docker.com/r/datamechanics/spark), adding jars, python libraries, and drivers to your environment, building a simple Pyspark application that reads a public data source and leverages [Koalas](https://koalas.readthedocs.io/en/latest/) transformations to find the median of the dataset, and writes the results to a Postgres instance. To help you get started, we've included a justfile with some simple commands:

- To build your docker image locally, run `just build`
- To run the Pyspark application, run `just run`
- To access a Pyspark shell in the docker image for debugging or running independent spark commands, run `just shell`

What you'll need:
- Docker installed - https://docs.docker.com/get-docker/
- A running Postgres instance with a table containing two columns, `etl_time DATETIME population DECIMAL(18,2)`
-- You can use the following command to create the table:
-- `create table <schema>.<table> (etl_time timestamp, population numeric(18,2));`
- AWS Credentials (AWS still requires access key and secret for public datasets)