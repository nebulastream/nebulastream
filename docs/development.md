# Development

This document explains how to set up the development environment for NebulaStream.
We distribute a development container image to enable students and external researchers to start hacking on the system as quickly
as possible. Additionally, for long-term developers or developers
disliking developing in a containerized environment, this document explains how to set up
and build NebulaStream in a non-containerized environment.

## Development Container

The recent (based on the main branch) development image can be pulled from

> docker pull nebulastream/nes-development:latest

The image contains a llvm-18-based toolchain (with libc++), a recent CMake version, the mold linker and
a pre-built development sdk based on the vcpkg manifest.

This container image can be integrated into, e.g., CLion via a docker-based toolchain or used via the command line.

> docker run --workdir /home/ubuntu/nes -v $(pwd):/home/ubuntu/nes nebulastream/nes-development:176-Dependency cmake -B
> build-docker -S .

> docker run --workdir /home/ubuntu/nes -v $(pwd):/home/ubuntu/nes nebulastream/nes-development:176-Dependency cmake
> --build build-docker -j

Internally the docker container is running as user `ubuntu`, if you run into issues related to permissions of the
`build-docker` folder you may have to adjust the user used in the docker container, e.g., by using the flag
`-u $(id -u):$(id -g)`

### Dependencies via VCPKG

The development container has an environment variable `VCPKG_ROOT`, which, once detected by the CMake build system, will configure the correct toolchain to use. 

Since the development environment only provides a pre-built set of dependencies, changing the dependencies in the containerized mode is impossible. If desired, a new development image can be built locally or via the CI.

### CCache

If you want to use `CCache`, you need to mount your host machines' CCache directory (`ccache -p`) into the docker
containers CCache directory.

```
docker run \
    --workdir /home/ubuntu/nes \
    -v ${HOME}/.cache/ccache:/home/ubuntu/.cache/ccache \
    -v $(pwd):/home/ubuntu/nes \
    nebulastream/nes-development:176-Dependency \
    cmake -B build-docker -S .
```

## Non-Container Development Environment

The relevant CI Jobs will be executed in the development container. This means in order to reproduce CI results, it is essential to replicate the development environment built into the base docker image.

The compiler toolchain is based on `llvm-18` and libc++-18, and we use the mold linker for its better performance.

### Dependencies are provided via VCPKG

A local vcpkg repository should be created, which could be shared between different projects. To instruct the CMake build to use a local vcpkg repository, pass the vcpkg CMake toolchain file to the CMake configuration command.

> cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=/home/user/vcpkg/scripts/buildsystems/vcpkg.cmake

The first time building NebulaStream, VCPKG will build all dependencies specified in the vcpkg/vcpkg.json manifest,
subsequent builds can rely on the VCPKGs internal caching mechanisms even if you delete the build folder.

If you omit the toolchain file, the CMake system will create a local vcpkg-repository inside the project directory
and pursue building the dependencies in there; if you later wish to migrate the vcpkg-repository you can move it elsewhere on your system and specify the `-DCMAKE_TOOLCHAIN_FILE` flag.
