image_name := 'gcr.io/dm-docker/demo/scala:dev'
jar_name := 'application.jar'
work_dir := '/opt/spark/work-dir'
cluster_url := 'https://demo.datamechanics.co'
service_account := env_var('HOME') + '/.config/gcloud/application_default_credentials.json'
api_key := 'null'

# Build the JAR
build_jar:
    sbt assembly

# Build the image
build_image:
    docker build -t {{image_name}} .

# Build everything and push the image to the registry
update_registry: build_jar build_image
    docker push {{image_name}}

# Run locally with JAR mounted
_run_locally_mounted main_class +args='':
    docker run --mount type=bind,source="$(pwd)",target={{work_dir}} \
               --mount type=bind,source={{service_account}},target=/sa.json \
               --env GOOGLE_APPLICATION_CREDENTIALS=/sa.json \
        {{image_name}} driver --class {{main_class}} local://{{work_dir}}/target/scala-2.12/{{jar_name}} {{args}}

# Run locally with JAR baked-in
_run_locally main_class +args='':
    docker run \
        {{image_name}} driver --class {{main_class}} local://{{work_dir}}/{{jar_name}} {{args}}

# Run JAR locally
run_spark_pi_locally arg='10': (_run_locally 'co.datamechanics.pi.Main' arg)

# Run JAR locally without needing to rebuild the image
run_spark_pi_locally_mounted arg='10': (_run_locally_mounted 'co.datamechanics.pi.Main' arg)

# Run JAR locally without needing to rebuild the image
run_wordcount_locally_mounted input output: (_run_locally_mounted 'co.datamechanics.wordcount.Main' input + ' ' + output)

# Run JAR on Data Mechanics
run_spark_pi_on_cluster arg='100': (_run_on_cluster 'scala-spark-pi' 'co.datamechanics.pi.Main' arg)

# Run JAR on Data Mechanics
run_wordcount_on_cluster input output: (_run_on_cluster 'scala-wordcount' 'co.datamechanics.wordcount.Main' '"' + input + '", "' + output + '"')

# Run JAR on Data Mechanics
_run_on_cluster job_name main_class +args='':
    #!/bin/bash
    output=$(curl -s --request POST {{cluster_url}}/api/apps/ \
        --header 'Content-Type: application/json' \
        --header 'X-API-Key: {{api_key}}' \
        --data-raw '{
          "jobName": "{{job_name}}",
          "configOverrides": {
            "type": "Scala",
            "sparkVersion": "3.0.1",
            "image": "{{image_name}}",
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local://{{work_dir}}/{{jar_name}}",
            "mainClass": "{{main_class}}",
            "arguments": [{{args}}]
          }
        }')
    echo $output | jq -r
    app_name=$(echo $output | jq -r '.appName')
    echo "Check out the app at {{cluster_url}}/dashboard/apps/$app_name"
    just api_key={{api_key}} cluster_url={{cluster_url}} _stream_logs $app_name

# Get the status of an app
_get_app_state app_name:
    @curl -s --request GET {{cluster_url}}/api/apps/{{app_name}} \
        --header 'Content-Type: application/json' \
        --header 'X-API-Key: {{api_key}}' | jq -r '.status.state'

# Print the live driver log stream of an app
_stream_logs app_name:
    #!/bin/bash
    app_state="null"
    echo {{app_name}}
    while [ "$app_state" = "null" -o "$app_state" = "SUBMITTED" -o "$app_state" = "" ]
    do
        app_state=$(just api_key={{api_key}} cluster_url={{cluster_url}} _get_app_state {{app_name}})
        echo "App is in state $app_state, waiting..."
        sleep 1
    done
    curl --request GET {{cluster_url}}/api/apps/{{app_name}}/live/driver-log \
        --header 'Content-Type: application/json' \
        --header 'X-API-Key: {{api_key}}'
