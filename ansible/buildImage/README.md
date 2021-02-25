# Nebula Build deployment script

## Prerequisites

### Requirements:

Pip install the following versions:
- `ansible==2.4.2.0`
- `docker-py==1.9.0`

Here we recommend that the libraries be installed with **pip install** instead of apt-get install, as you have more flexibility with selecting library version as you wish.

If errors occur due to version incompatability, please try the following commands (take ansible as example):

```sh
sudo apt-get remove ansible
sudo apt purge ansible
pip uninstall ansible
sudo apt-get update
sudo pip install ansible==2.4.2.0
```

If errors happen because you have anaconda python3 installed and ansible won't start, please try the following command:
```sh
conda deactivate
```


### Quick start

Before running any command please make sure that your local copy of the ansible_plays repo contains the following file:
- `vault_pass.txt`
This path is .gitignored, you need to obtain this file from some other source (ask your admin).
- Also add this file in your user home directory in a `credentials` folder (ex: `jiacheng/credentials/vault_pass.txt`)
in the machine you want to deploy to.

To deploy on the server you need to add the user public ssh key in the server in `~/.ssh/authorized_keys`.

For deployement, cd to this `buildImage` folder, we use of a `Makefile` that has the following commands:
#### Build Base image (for nebula_build)
```
$ make build
```
#### Build Tool image (for nebula_build)
```
$ make deploy_nebula
```
#### Run Tool container
It gives you the choice to run the container you select.
```
$ make run
```

### Params
When you run these playbooks you will be prompted for the following variables:
- `host_group`: `ds` (remote server) or `localhost` (on your local machine).
- `remote_user`: user name for which you have access and ssh key stored on the server.
- `build_base_image` or `to_deploy_app`: this is for the building of the images.
