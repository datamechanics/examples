# Data Mechanics integration with JupyterHub in Docker on single machine

This is an example integration of a JupyterHub integration with the DataMechanics platform deploying JupyterHub on a single machine instance using Docker.


## Usage

Run `make run` and browse to http://localhost:8000/.
Log in with user `test` and password `test`.

Do not use this image and configuration as is in production. JupyterHub authentication is not safe (it uses the built-in `LocalAuthenticator` with a dummy test user).

## Explanation

* JupyterHub runs in a Docker container
* The JupyterHub configuration is mounted in the Docker container as part of a volume
* Jupyter notebook servers are run in separate containers thanks to the `DockerSpawner`
* The integration with Data Mechanics consists in subclassing the `DockerSpawner` to add the Data Mechanics URL and the API key as arguments of the spawner Jupyter notebook server Docker container
