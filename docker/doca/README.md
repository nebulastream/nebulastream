# DOCA build container w/ NES

## First steps
1. `sudo apt-get install qemu binfmt-support qemu-user-static`
2. `sudo docker run --rm --privileged multiarch/qemu-user-static --reset -p yes`

## Raw image
1. `docker pull nvcr.io/nvidia/doca/doca:2.6.0-devel`