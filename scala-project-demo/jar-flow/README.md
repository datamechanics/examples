# Scala dev workflow using an object store

This folder proposes an example of dev workflow for Scala projects on Data Mechanics.

It pushes the JAR built locally on an object store (GCS) and configures the application on Data Mechanics to use it.

This folder uses `just` as a command runner (it's a simpler replacement for make).

```bash
$ just --list
Available recipes:
    run_spark_pi_locally +arg='10' # Run JAR locally
    run_spark_pi_on_cluster +arg='100' # Run JAR on Data Mechanics
    update_jar                     # Build the JAR and push it to GCS
```

Variables:

```bash
$ just --evaluate
api_key       := "null"
cluster_url   := "https://demo.datamechanics.co"
jar_name      := "datamechanics-demo-assembly-0.1.0-SNAPSHOT.jar"
remote_folder := "gs://dm-demo-data/code"
```

## JAR content

The JAR contains two examples:
* a word count example in classes `co.datamechanics.wordcount.Main` and `co.datamechanics.wordcount.LocalMain`
* a Monte-Carlo estimation of Pi in classes `co.datamechanics.pi.Main` and `co.datamechanics.pi.LocalMain`

`LocalMain` classes set the `master` to `local` so that the application can run locally for testing purpose.

## Example

To run the app locally:

```bash
just run_spark_pi_locally 10
```

To run the app on a Data Mechanics cluster:

```bash
just api_key=<your_api_key> cluster_url='https://dp-xxxxxxxx.datamechanics.co' run_spark_pi_on_cluster 100
```
