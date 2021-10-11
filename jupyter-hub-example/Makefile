.PHONY: install

.helm-init-done:
	helm repo update
	helm repo add jupyterhub https://jupyterhub.github.io/helm-chart/
	touch .helm-init-done

install-singleuser: .helm-init-done
	helm upgrade \
		--cleanup-on-fail \
		--install jupyterhub jupyterhub/jupyterhub \
		--namespace jupyterhub \
		--create-namespace \
		--version 0.11.1 \
		--values config-singleuser.yaml

install-profiles: .helm-init-done
	helm upgrade \
		--cleanup-on-fail \
		--install jupyterhub jupyterhub/jupyterhub \
		--namespace jupyterhub \
		--create-namespace \
		--version 0.11.1 \
		--values config-profiles.yaml
