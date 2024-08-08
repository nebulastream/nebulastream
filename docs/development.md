# Development

This document explains how to set up the development environment for NebulaStream.
We distribute a development container image to enable anyone interested hacking on the system as quickly
as possible. Additionally, for long-term developers or developers
disliking developing in a containerized environment, this document explains how to set up
and build NebulaStream in a non-containerized environment.

## Development Container

The recent (based on the main branch) development image can be pulled from

> docker pull nebulastream/nes-development:latest

The image contains an LLVM-based toolchain (with libc++), a recent CMake version, the mold linker and
a pre-built development sdk based on the vcpkg manifest.

This container image can be integrated into, e.g., CLion via a docker-based toolchain or used via the command line.

To configure the cmake build into a `build` directory, you have to mount the current working directory `pwd` into the
container. Additional cmake flags can be appended to the command.

> docker run --workdir /home/ubuntu/nes -v $(pwd):/home/ubuntu/nes nebulastream/nes-development:latest cmake -B
> build-docker -S .

The command to execute the build also requires, the current directory to be mounted into the container.

> docker run --workdir /home/ubuntu/nes -v $(pwd):/home/ubuntu/nes nebulastream/nes-development:latest cmake
> --build build-docker -j

Internally the docker container is running as user `ubuntu`, if you run into issues related to permissions of the
`build-docker` folder you may have to adjust the user used in the docker container, e.g., by using the flag
`-u $(id -u):$(id -g)`

> docker run -u \$(id -u):\$(id -g) --workdir /home/ubuntu/nes -v $(pwd):/home/ubuntu/nes nebulastream/nes-development:
> latest cmake -B
> build-docker -S .

### Dependencies via VCPKG

The development container has an environment variable `VCPKG_ROOT`, which, once detected by the CMake build system, will
configure the correct toolchain to use.

Since the development environment only provides a pre-built set of dependencies, changing the dependencies in the
containerized mode is impossible. If desired, a new development image can be built locally or via the CI.

### CCache

If you want to use `CCache`, you need to mount your host machines' CCache directory (`ccache -p`) into the docker
containers CCache directory.

```
docker run \
    --workdir /home/ubuntu/nes \
    -v ${HOME}/.cache/ccache:/home/ubuntu/.cache/ccache \
    -v $(pwd):/home/ubuntu/nes \
    nebulastream/nes-development:latest \
    cmake -B build-docker -S .
```

### CLion Integration

To integrate the container-based development environment you need to create a new docker-based toolchain.
With the following settings:

![CLion-Toolchain-Settings](resources/SetupDockerToolchainClion.png)

This configuration assumes ccache is using the default directory of `${HOME}/.cache/ccache`. You can create additional
docker-based toolchains if you plan to experiment with different sanitizer.

Lastly, you need to create a new CMake profile which uses the newly created docker-based toolchain:

![CLion-CMake-Settings](resources/SetupDockerCmakeClion.png)
## Non-Container Development Environment

The relevant CI Jobs will be executed in the development container. This means in order to reproduce CI results, it is
essential to replicate the development environment built into the base docker image. Note that NebulaStream uses llvm
for its
query compilation, which will take a while to build locally.

The compiler toolchain is based on `llvm-18` and libc++-18, and we use the mold linker for its better performance.
Follow the
[llvm documentation](https://apt.llvm.org/) to install a recent toolchain via your package manager.

### Local VCPKG with DCMAKE_TOOLCHAIN_FILE

A local vcpkg repository should be created, which could be shared between different projects. To instruct the CMake
build to use a local vcpkg repository, pass the vcpkg CMake toolchain file to the CMake configuration command.

> cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=/home/user/vcpkg/scripts/buildsystems/vcpkg.cmake

The first time building NebulaStream, VCPKG will build all dependencies specified in the vcpkg/vcpkg.json manifest,
subsequent builds can rely on the VCPKGs internal caching mechanisms even if you delete the build folder.

### Local VCPKG without DCMAKE_TOOLCHAIN_FILE

If you omit the toolchain file, the CMake system will create a local vcpkg-repository inside the project directory
and pursue building the dependencies in there. If you later wish to migrate the vcpkg-repository you can move it
elsewhere on your system and specify the `-DCMAKE_TOOLCHAIN_FILE` flag.
