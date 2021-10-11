curl --request POST https://aws-test.datamechanics.co/api/apps/ \
        --header 'Content-Type: application/json' \
        --header 'X-API-Key: 6e2f9a502c70a8828fecc6ffe9b866f0e53c2b7757d2aa450ea0fc6e3f1fcff4' \
        --data-raw '{ 
          "jobName": "sparklyr", 
          "configTemplateName": "sparklyr2"
         }'
