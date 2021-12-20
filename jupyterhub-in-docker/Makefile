build:
	docker build -t hub .

run: build
	docker run --rm -p 8000:8000 -v $(PWD):/srv/jupyterhub -v /var/run/docker.sock:/var/run/docker.sock hub
