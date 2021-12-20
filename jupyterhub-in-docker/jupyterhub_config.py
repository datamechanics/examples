import netifaces
from dockerspawner import DockerSpawner
from traitlets import Unicode


class DataMechanicsDockerSpawner(DockerSpawner):
    """A KubeSpawner subclass that launches notebook kernels on the
    Data Mechanics platform rather than on a Jupyter server pod.
    """
    datamechanics_url = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        The URL to your Data Mechanics platform notebook API.
        Typically: https://dp-xxxxxxxx.datamechanics.co/notebooks/
        """,
    )

    async def options_from_form(self, form_data):
        options = {}
        options["datamechanics_api_key"] = form_data[f"api_key"][0]
        return options

    def get_args(self):
        datamechanics_api_key = self.user_options["datamechanics_api_key"]
        args = super().get_args() + [
            f"--gateway-url={self.datamechanics_url}",
            f"--GatewayClient.auth_token={datamechanics_api_key}",
            "--GatewayClient.request_timeout=600",
        ]
        return args

c.JupyterHub.spawner_class = DataMechanicsDockerSpawner
c.DataMechanicsDockerSpawner.datamechanics_url = "https://dp-23d3078f.datamechanics.co/notebooks/"
c.DataMechanicsDockerSpawner.options_form = """
    Please enter a Data Mechanics API key: <input name="api_key" />
"""

# Some general configuration for DockerSpawner
# (a Spawner that runs Jupyter services in separate Docker containers)
c.DockerSpawner.image = "jupyterhub/singleuser:latest"
c.DockerSpawner.remove_containers = True
c.DockerSpawner.remove = True


# Some networking configuration to make Jupyterhub in Docker
# compatible with DockerSpawner
docker0 = netifaces.ifaddresses('eth0')
docker0_ipv4 = docker0[netifaces.AF_INET][0]
network_name = "bridge"
c.JupyterHub.hub_ip = '0.0.0.0' #docker0_ipv4['addr']
c.JupyterHub.hub_connect_ip = docker0_ipv4['addr']
c.DockerSpawner.use_internal_ip = True
c.DockerSpawner.network_name = network_name
c.DockerSpawner.extra_host_config = { 'network_mode': network_name }
