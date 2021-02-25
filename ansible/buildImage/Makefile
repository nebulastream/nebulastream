all: build

build:
	ansible-playbook build.yml --vault-password-file vault_pass.txt -i hosts

run:
	ansible-playbook run.yml --vault-password-file vault_pass.txt -i hosts

deploy_nebula:
	ansible-playbook deploy_nebula.yml --vault-password-file vault_pass.txt -i hosts
