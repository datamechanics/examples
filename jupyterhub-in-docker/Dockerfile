FROM jupyterhub/jupyterhub

# This is unsafe. It's just a way to have a user "test"
# with password "test" for JupyterHub.
# By default JupyterHub uses the LocalAuthenticator that uses UNIX users.
RUN useradd --create-home test
RUN echo 'test:test' | chpasswd

RUN pip install dockerspawner netifaces
