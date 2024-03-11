# DOCA build container w/ NES
This is basically a bare 22.04 Ubuntu with NVidia libraries.

## First steps
1. `sudo apt-get install qemu binfmt-support qemu-user-static`
2. `docker run --rm --privileged multiarch/qemu-user-static --reset -p yes`

## Add NES layer
1. `docker buildx build . -t nebulastream/nes-doca --load`

## Raw image (in case it's needed)
1. `docker pull nvcr.io/nvidia/doca/doca:2.6.0-devel`