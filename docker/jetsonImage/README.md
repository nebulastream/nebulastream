This folder contains scripts to build a NebulaStream executable Docker image with support to run OpenCL kernels via POCL on the Nvidia Jetson architecture.

The Docker image is based on the image nvcr.io/nvidia/l4t-base:r32.3.1, but upgraded to Ubuntu 20.04. POCL is installed at version 1.7. NebulaStream and POCL are built with LLVM 12 installed from the Ubuntu APT repositories.

The script `build-all.sh` contains all the build commands. It should be run from the root of a checked out NebulaStream repository.