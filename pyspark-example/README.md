This tutorial demonstrates the process of building a Spark Docker image from one of Data Mechanic's [base images](https://hub.docker.com/r/datamechanics/spark), adding jars, python libraries, and drivers to your environment, building a simple Pyspark application that reads a public data source and leverages [Koalas] transformations to find the median of the dataset, and writes the results to a Postgres instance. We've included a justfile to help you get started:
- To build your docker image locally, run `just build`
- To run the Pyspark application, run `just run`
- To access a Pyspark shell in the docker image for debugging or running independent spark commands, run `just shell`

What you'll need:
- Docker installed - https://docs.docker.com/get-docker/
- 
