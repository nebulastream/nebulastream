# About

This readme file explains about how you can build and publish the artifacts on our nebulastream docker hub repository.
Currently, we have two images on our repository: a build image used by travis and an executable image used for exdra project.   

(Note: Please make sure you have rights to publish images on [nebulastream](https://hub.docker.com/u/nebulastream) organization. Please ask one of the core developers to grant you access if you do not have one.)

# Build

This section describes how to build a docker image locally. We have currently two docker files one for each docker image,
describing how to build the docker images. In case, you want to add or modify any dependencies in the docker image please 
edit the corresponding docker file. Also, the startup script for each build image is present inside the [scripts](\scripts) 
folder. 

The [entrypoint-nes-build.sh](\scripts\entrypoint-nes-build.sh) defines how the NebulaStream build image will 
run when started and the [entrypoint-nes-executable.sh](\scripts\entrypoint-nes-executable.sh) will define how the executable 
image will start. If you want to change the startup behavior of the docker images, please change the corresponding entrypoint script.   

## Building build image

Please execute following command for building the build image locally:

`docker build . -f Dockerfile-NES-Build -t nebulastream/nes-build-image:latest`

## Building execution image

For build image, we install NebulaStream using deb package inside the [resources](\resources) folder.
If you want to update the NebulaStream, please create a new deb package by compiling the code inside the docker image and running `cpack` command.
Afterwards, please replace the deb package inside the resources folder and follow the instruction below.

Please execute following command for building the executable image locally:

`docker build . -f Dockerfile-NES-Executable -t nebulastream/nes-executable-image:latest`

***
_(Note: Please replace **_latest_** with any other value if you do not want to replace the original image and wanted to create your own version of the docker image)_
***

# Publish

This section describes, how to publish the image to docker repository. Fist, please login into your docker account locally by executing :

`docker login`

(Note: it is important for you to have access to nebulastream docker hub organization for further steps)

## Building build image

Please execute following command for publishing the build image on docker hub:

`docker push nebulastream/nes-build-image:latest`

## Building execution image

Please execute following command for building the executable image locally:

`docker push nebulastream/nes-executable-image:latest`

***
_(Note: Please replace **_latest_** with any other value if you do not want to replace the original image and wanted to publish your own version of the docker image)_
***
