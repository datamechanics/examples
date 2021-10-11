# Scala dev workflow using a Docker image

This folder proposes an example of dev workflow for Scala projects on Data Mechanics.

It pushes a Docker image built locally and containing the application JAR to a Docker registry.
It configures the application on Data Mechanics to use it.

This folder uses `just` as a command runner (it's a simpler replacement for make).

```bash
$ just --list
Available recipes:
    build_image                   # Build the image
    build_jar                     # Build the JAR
    run_spark_pi_locally arg='10' # Run JAR locally
    run_spark_pi_locally_mounted arg='10' # Run JAR locally without needing to rebuild the image
    run_spark_pi_on_cluster arg='100' # Run JAR on Data Mechanics
    run_wordcount_locally_mounted input output # Run JAR locally without needing to rebuild the image
    run_wordcount_on_cluster input output # Run JAR on Data Mechanics
    update_registry               # Build everything and push the image to the registry
```

Variables:

```bash
$ just --evaluate
api_key         := "null"
cluster_url     := "https://demo.datamechanics.co"
image_name      := "gcr.io/dm-docker/demo/scala:dev"
jar_name        := "application.jar"
service_account := "$HOME/.config/gcloud/application_default_credentials.json"
work_dir        := "/opt/spark/work-dir"
```

## JAR content

The JAR contains two examples:
* a word count example in classes `co.datamechanics.wordcount.Main` and `co.datamechanics.wordcount.LocalMain`
* a Monte-Carlo estimation of Pi in classes `co.datamechanics.pi.Main` and `co.datamechanics.pi.LocalMain`

The `LocalMain` classes are not useful for this example because the Docker image run locally uses the `Main` classes.

## Spark Pi

To run the app locally:

```bash
just run_spark_pi_locally 10
```

To run the app locally without needing to rebuild the image:
To run the app locally:

```bash
just run_spark_pi_locally_mounted 10
```
In this case, the JAR will be mounted in the Docker container.
Note that you need to rebuild the JAR to update the application.

To run the app on a Data Mechanics cluster:

```bash
just api_key=<your_api_key> cluster_url='https://dp-xxxxxxxx.datamechanics.co' run_spark_pi_on_cluster 100
```

## Wordcount

To run the app locally:

```bash
just run_wordcount_locally_mounted README.md output
```

You can also use remote data locally:

```bash
just run_wordcount_locally_mounted gs://path/to/data output
```
The script mounts `$HOME/.config/gcloud/application_default_credentials.json` into the Docker container so that Spark has the same permissions as the default user on the local machine.

If the default gcloud credentials are not located here, please override with:
```bash
just service_account=path/to/service/account.json run_wordcount_locally_mounted gs://path/to/data output
```

To run the app on a Data Mechanics cluster:

```bash
just api_key=<your_api_key> cluster_url='https://dp-xxxxxxxx.datamechanics.co' \
    run_wordcount_on_cluster gs://path/to/data 100
```
You'll have to use [your own Docker registry](https://docs.datamechanics.co/docs/packaging-code) and make sure the Data Mechanics cluster has access to it.
