# The Problem

The NES Build System has grown over the years. The refactor allows us to revisit how we handle the modularization of modules, dependencies, and different build configurations.

Currently, dependencies are managed via four different channels:
- Some smaller libraries are vendored directly in-tree (magic_enum, yaml, backwards)
- Some smaller utils are fetched via FetchContent during configuration. (guardonce, tpch, flounder)
- Some (usually) larger libraries are downloaded during the initial CMake configuration. These dependencies are built in an external Repository using the VCPKG manager.
- The libraries and binaries of the LLVM compiler toolchain are build in a different external-repository and also downloaded during build configuration.


## Problems with In-tree vendored dependencies

For locally vendored libraries, we may run into licensing issues if we don't separate them from our source files. This is mostly due to different licensing requirements. Some require us to keep them unmodified, and all require us to keep the original file header, which means they have to be excluded from our license header checks and other automatic tooling.

An additional issue is that we potentially lose version information; it is impossible to trace back which version of the library we are using, which leaves us guessing if there are updated versions, what features are supported, and which documentation to use.

## Problem with FetchContent

NebulaStream applies strict warnings during the CMake build process by adding all reasonable compiler warnings and declares warnings to be treated as Errors. FetchContent effectively builds third-party dependencies as if they are part of our system propagating all compiler flags; this leaves many third parties unable to build due to the strict warnings.

There might be ways around this by carefully setting warning flags to all CMake targets, but this could cause issues in the long run where certain modules forget to add the warning flags, instead using the global approach to guarantee all CMake targets are built with a consistent set of warnings appears to be the sane approach.

## Problems with VCPKG

When using VCPKG we are limited to the set of currently supported libraries in VCPKG (which is quite large). 
A potential issue, and probably the reason why the external-repository approach has been used in the past, is the fact that VCPKG just provides a standardized collection of build scripts to configure and build dependencies locally. This means whenever someone wants to build NebulaStream, all its dependencies need to be built first. However, this is also true for the previous two approaches. Currently, VCPKG does not offer a binary cache out of the box, but [some implementation](https://github.com/lesomnus/VCPKG-cache-http) exists that could run self-hosted. 

VCPKG, however, allows the specification of different toolchains that could be used to build dependencies with different sets of flags, which would require building NebulaStream properly with sanitizers.

## Problem with External-Repository

Currently, many dependencies are built in an [external repository](https://github.com/nebulastream/nebulastream-dependencies/). This has the advantage that it is hidden from developers who want to build NebulaStream. Still, it also has the disadvantage that the repository is widely unknown and requires special knowledge. The dependencies have not been updated for over 2.5 years. Adding or changing new dependencies or experimenting with flags is difficult because you have to handle two separate repositories with different release flows.

Especially if we plan to have more build configurations for different types of runtime checkers in the future, we need to allow the dependencies to be built with a more flexible set of configurations. Updating dependencies should happen more frequently.

### LLVM and Clang

Currently, we build LLVM and clang in an external repository. The CMake configuration downloads our version of Clang and sets up the CMake compiler toolchain accordingly. Specifying a very concrete toolchain during our build brings the advantage of not only supporting a specific toolchain but also losing diagnostics and robustness of using different compilers.

# Goals and Non-Goals

(**G1**) Easy onboarding for new developers:  After cloning the repository, the developer should be able to build the system following the build guide, without running into issues with missing locally installed libraries, or missing toolchains. We achieve this by offering docker based Development Environment. The build guide explains how to configure the Development Image in CLion, the CMake build system will build without further configuration.

(**G2**) Runtime Analyzer, like sanitizer, requires changes to our dependencies: It should be possible to add additional build configurations for our dependencies. Currently, we use different VCPKG toolchains for x64, arm, Linux, and macOS for all of these configurations, we require the dependencies to build and link with different sanitizer flags: `-fsanitize=thread`, etc...

(**G3**) Overview of Dependencies: One central source of truth over which dependencies are used, which version, and if there are patches.

(**G4**) Easy modification: Dependencies should be extendable (or shrinkable), upgradable, and modifiable (with custom patches).  It should not require cross-repository efforts to change dependencies and create mismatches between 

(**G5**) Enable alternative tooling: different compilers and different standard libraries.

(**G6**) Automatic detection of modification to the dependencies in the PR-Review process.

(**NG1**) Remove dependencies: Shifting to a more easily manageable dependency and improving the overall encapsulation of modules and their dependencies should allow future efforts to remove dependencies, but it won't be a focus here.

(**NG2**) Offer support for currently unsupported platforms: While new compilers might be supported, this does not include supporting them on any platform. The plan is to offer a docker Dev-Image, which can be used if the system's tooling is not currently supported.

# Proposed Solution

We want to shift all dependencies exclusively into VCPKG. The `vcpkg-repository` can be maintained independently or via a submodule in the CMake build directory. A `vcpkg` directory in the root directory will list all dependencies in the `vcpkg.json` manifest and the currently used VCPKG `builtin-baseline`.

```
nebulastream/
    grpc
    cmake
    ...
    vcpkg-repository/... <-- submodule
    vcpkg/                 <-- relevant configuration
        vcpkg.json
        triplets/
            x64-linux-asan.cmake
            ...
        ports/
            folly
```

We use the VCPKG Manifest mode to keep a `sbom` in the `vcpkg.json,` which contains all dependencies (**G3**). The actual version can either be fixed using an override or be the recent version of the VCPKG manifest-baseline commit.
```json
{
  "name": "nebulastream-dependencies",
  "version-string": "main",
  "homepage": "https://nebula.stream",
  "description": "Data Management for the Internet of Things.",
  "supports": "x64 & linux",
  "builtin-baseline": "01f602195983451bc83e72f4214af2cbc495aa94", 
  "dependencies": [
    "grpc",
    "fmt",
    "gtest",
    "benchmark",
    "spdlog"
  ],
  "overrides": [
    {
      "name": "zeromq",
      "version": "4.3.4",
      "port-version": 6
    }
  ]
```

Bumping the VCPKG `builtin-baseline` to a more recent commit updates dependencies (**G4**,  **G6**). However, incompatibilities can still arise, which may require conversions to be pinned to specific versions.

During the initial CMake configuration, the build system checks if a `-DCMAKE_TOOLCHAIN_FILE` was provided, which could point to an independently maintained `vcpkg-repository` somewhere on the developer's system. If the flag is not present, the CMake configuration will checkout, bootstrap the VCPKG repository, and manually set up the toolchain path before initializing the CMake project.

```cmake
if (CMAKE_TOOLCHAIN_FILE)
    message(STATUS "Using independent vcpkg-repository: ${CMAKE_TOOLCHAIN_FILE}")
else ()
    message(STATUS "Using internal vcpkg-repository")
    # setup...
    SET(CMAKE_TOOLCHAIN_FILE ${CLONE_DIR}/scripts/buildsystems/vcpkg.cmake)
endif ()

```

The internal VCPKG toolchain (identified by the host triplet) needs to be set before the Project initialization, this means CMake Options regarding sanitizers and other hardening techniques influencing the choice of dependencies need to be evaluated before initializing the CMake project as well. (**G5**, **G2**)

```cmake
elseif (NES_ENABLE_ADDRESS_SANITIZER)
    MESSAGE(STATUS "Enabling Address Sanitizer")
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
    SET(VCPKG_HOST_TRIPLET "x64-linux-asan") # <-- use the asan toolchain
else ()
    MESSAGE(STATUS "Enabling No Sanitizer")
    SET(VCPKG_HOST_TRIPLET "x64-linux")
endif ()

SET(VCPKG_MANIFEST_DIR "${CMAKE_SOURCE_DIR}/vcpkg")
SET(VCPKG_OVERLAY_TRIPLETS "${CMAKE_SOURCE_DIR}/vcpkg/triplets")
SET(VCPKG_OVERLAY_PORTS "${CMAKE_SOURCE_DIR}/vcpkg/ports")

project(nes)
```
This would allow the new developer to set up the NebulaStream system using a plain `cmake -B build -S . ` command, which would internally fetch the VCPKG-registry(**G1**). It would also allow developers already using VCPKG, maybe for a different project, to reuse an already locally installed VCPKG registry `cmake -B build -S .  -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/vcpkg.cmake`

To enable dependencies to be built with different compiler flags (Sanitizer, Hardening Mode), the toolchain files inside the `VCPKG/toolchain` folder set a VCPKG toolchain. usually, this just includes setting additional C/C++ Flags propagated to the individual dependency builds.

Contents of `x64-linux-asan.cmake` 
```cmake
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
set(VCPKG_CXX_FLAGS "-std=c++20 -fsanitize=address")
set(VCPKG_C_FLAGS "-fsanitize=address")
```

## Clang and LLVM

In its current state, we require the clang binary to compile NebulaStream queries and the C++-Backend; on the other side, we require LLVM and MLIR for the MLIR backend. We could investigate to lift the strict requirement of a fixed C++ clang compiler and thus drop the Clang dependency. LLVM and MLIR are supported within VCPKG and can thus be built like any other dependency. The development docker image still contains the well-working clang compiler version.

# Proof Of Concept

The [NebuLI](https://github.com/ls-1801/NebuLI) project uses the approach. Using a local VCPKG repository requires the user to specify it as a CMake configuration parameter.

> cmake -DCMAKE_TOOLCHAIN_FILE=/PATH/TO/VCPKG/scripts/buildsystems/vcpkg.cmake

If no local VCPKG toolchain file was specified, the CMake system creates a new VCPKG registry and starts a fresh dependency build.

Lastly, a docker image at `luukas/nebula stream:alpine` contains a set of prebuilt dependencies and the required build tools.

> docker pull docker pull luukas/nebulastream:alpine

A new docker-based toolchain needs to be created in CLion. A CMake build inside the docker container will pick up the destination of the pre-installed VCPKG-registry based on the `NES_PREBUILT_VCPKG_ROOT` environment variable, so no further CMake configuration parameters are required.


# Open Questions

## Missing libraries dependencies

The `VCPKG` registry can be extended, offering templates for the most common software distribution methods. Arguably, using `FetchContent` in the NebulaStream CMake configuration would be simpler, but allowing the VCPKG.json to be local in the NebulaStream repository removes some of the impedance previously encountered when managing two external dependencies.

## Binary Caching

In the current iteration, the initial VCPKG would require the VCPKG dependencies to be built locally (for each toolchain), this is a significant time investment we impose on any new developer. To prevent this we could host a binary cache for dependencies: [VCPKG-cache-http](https://github.com/lesomnus/VCPKG-cache-http), but this would only be available internally to DIMA.

An alternative would be integrating a prepopulated VCPKG registry into our docker development image. When we distribute the development docker image to use in `CLion,` the developer needs to specify the CMAKE_TOOLCHAIN_FILE, which resides internally in the docker container. The docker development image needs to be rebuilt whenever we detect changes to the dependencies. 

