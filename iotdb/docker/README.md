# About

This readme file explains about how you can build and publish the artifacts on our nebulastream docker hub repository.
Currently, we have two images on our repository: a build image used by travis and an executable image used for exdra project.   

(Note: Please make sure you have rights to publish images on [nebulastream](https://hub.docker.com/u/nebulastream) organization. Please ask one of the core developers to grant you access if you do not have one.)

## Images and Dockerfiles
We have three docker files, one for each docker image, describing how 
to build the docker images. In case you want to add or modify any dependencies in the docker image please 
edit the corresponding docker file. Also, the startup script for each build image is present inside the [scripts](\scripts) 
folder. 

#### Dockerfile-NES-Build
This is the docker image for our CI builds. There is no way to connect to a running container of this image, 
aside from `docker attach`. Its purpose is to build and exit. It can be found in [Build](Dockerfile-NES-Build).

The `ENTRYPOINT` is located in [entrypoint-nes-build.sh](\scripts\entrypoint-nes-build.sh). The entrypoint
checks if source code is indeed mounted in the correct location inside the
container and starts a `make_debug` test build.

#### Dockerfile-NES-Dev
This image is based on NES-Build-image. Additionally, we add connectivity and any interactivity tools to this image.
It has everything installed that we need for development, including `ssh` connectivity. Its purpose is to start
in the background while our IDEs connect to it for remote debugging purposes. It can be found in [Dev](Dockerfile-NES-Dev).

Currently, there is no need for an `ENTRYPOINT` for this image. 
It stars `sshd` and can be kept in the background. For more info, check
Docker's official docs [here](https://docs.docker.com/engine/examples/running_ssh_service/).

#### Dockerfile-NES-Executable
This is our executable image. It extends the Build image. There is no way to connect to a running container of this 
image, aside from `docker attach`. Its purpose is to offer a host operating system for an executable of NES.

For this image, we install NebulaStream using a `deb` package inside the [resources](\resources) folder.
If you want to update the NebulaStream binary, please create a new `deb` package by compiling the code inside the docker image and running `cpack` command.
Afterwards, replace the `deb` package inside the resources folder. The image can be found in [Executable](Dockerfile-NES-Executable).

Currently, the image is tasked with only running a `.deb` version of NebulaStream. This may change
in the future. Its `ENTRYPOINT` is located in [entrypoint-nes-executable.sh](\scripts\entrypoint-nes-executable.sh).

#### Changing running behavior of images
If you want to change the startup behavior of the docker images, please change the corresponding entrypoint script.   

## Build images

Please execute following command for building the build image locally:

`docker build . -f Dockerfile-NES-XXX -t nebulastream/nes-XXX-image:YYY`

Where `XXX` represents one image type out of:
- `devel`
- `build`
- `executable`

and `YYY` as one `tag` out of:
- `latest`
- `testing`

The image types have been explained.
For `tag`, we use `latest` for latest stable and `testing` for 
development purposes. If no `tag` is specified at build time,
then `latest` is the default.


## Publish images

This section describes, how to publish the image to docker repository. Fist, please login into your docker account locally by executing :

`docker login`

(Note: it is important for you to have access to nebulastream docker hub organization for further steps)

Afterwards, one can execute

`docker push nebulastream/nes-XXX-image:YYY`

Where `XXX` represents one image type out of:
- `devel`
- `build`
- `executable`

and `YYY` as one `tag` out of:
- `latest`
- `testing`

The image types have been explained.
For `tag`, we use `latest` for latest stable and `testing` for 
development purposes. If no `tag` is specified at build time,
then `latest` is the default.