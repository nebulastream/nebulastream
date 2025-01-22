# Dependency Management

This document outlines NebulaStream's current state of dependency management. All dependencies are
managed via [vcpkg](https://github.com/microsoft/vcpkg). The original design decisions are outlined in the
[dependency management design document](docs/design/20240710_dependency-management.md).

## VCPKG

### Manifest

All dependencies used by CMake are provided via vcpkg and managed via the vcpkg/vcpkg.json manifest file. We strive to
update the vcpkg baseline frequently to stay up to date and avoid major dependency upgrades. The VCPKG manifest allows
pinning to a specific version and offers fine-grained feature selection for large libraries like llvm. For more details,
we refer to the [vcpkg manual](https://learn.microsoft.com/en-us/vcpkg/concepts/manifest-mode).

### Ports

The official VCPKG repository does not provide all dependencies we may intend to use; it also will not provide future
in-house libraries (e.g. [nautilus](https://github.com/nebulastream/nautilus)). For these libraries, we provide custom
port files.

Additionally, we may have to introduce some changes to dependencies that are not suitable for upstreaming, or the
upstreamed changes may not be available via vcpkg. In this scenario, we also introduce a custom port, which could be
replaced with the official port in a future baseline update.

### Triplets

Although VCPKG offers binary caching, we usually build all dependencies from the source. This allows us
to control all compilation and linker flags, thus reducing the risk of incompatibilities. Additionally, this gives us
the flexibility to build with specialized compilation flags, like `-march=native`, sanitizers, or a different standard
library.

To control what flags are passed to the build of our dependencies, a custom vcpkg-triplet (e.g., x64-linux-nes)
overrides
defaults set by vcpkg. A CMake Toolchain file allows us to inject flags or choose a specific compiler toolchain or
linker.

More concretely [x64-linux-none-libcxx.cmake](../vcpkg/custom-triplets/x64-linux-none-libcxx.cmake), is a custom triplet
which includes
the [libcxx-toolchain.cmake](../vcpkg/custom-triplets/libcxx-toolchain.cmake) toolchain file. The triplet file can
modify specific
dependencies (vcpkg calls them ports), whereas the toolchain file is a general set of compiler options, i.e., it enables
building with the libc++ standard library. For example, we use the triplet file to enable building with different
sanitizers.
Bluntly adding the `-fsanitize=address` flags works for some dependencies; however, for some ports (llvm), we have to
use the CMake option instead to enable the sanitizer.

## Building NebulaStream

VCPKG is integrated into the NebulaStreams CMake build system configuration. The usually `find_package` calls are
initially resolved via vcpkg-managed dependencies. Running the initial CMake configuration, the developer has multiple
options for how NebulaStream detects the dependencies.
The developer passes a vcpkg toolchain file directly via
`-DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake`
The developer configures the CMake build in the developer container with the NES_PREBUILT_VCPKG_ROOT environment
variable prepared.
If the developer does not specify a toolchain file and no environment is set, the build system will clone a new
vcpkg-repository into the current working directory and set the toolchain file automatically.

It is helpful for a developer to create a local vcpkg repository outside the working directory and use the first
approach. This way, the vcpkg repository can be shared among multiple instances of NebulaStream or other projects using
vcpkg. Vcpkg ensures consistency between the manifest and the actual provided dependencies, especially when other
developers make changes to the dependencies or the toolchains.

Relying on a development container requires (although infrequent) updates to the container image if other developers
change the dependencies.

## Docker Images

The CI will create a development image based on the latest set of dependencies and toolchains. Internally, we built a
`base` image containing our compiler toolchain (clang, libc++, mold, CMake). The `base` image is used to build the
`dependency` image. We build multiple dependency images per triplet and platform. The `dependency` image build invokes
`vcpkg install` and `export's a prebuilt set of dependencies.

Lastly, the dependency image is used to build the development image, which contains additional tools we use in our CI or
during daily development (e.g., clang-format, clang-tidy, gdb, ...).

## Continuous Integration

The CI is aware of NebulaStreams dependency management. The PR CI Job detects changes to folders relevant to dependency
management within the set of changes in the pull request. Currently, these folders are `docker/dependency` and `vcpkg`.
If no changes to these folders are detected, the CI can skip the jobs building the docker images and use the most recent
development image (`latest`) to run the rest of the CI jobs relevant to PRs. The change detection is done by calculating
hashes of all relevant files. If we find a docker image with a matching hash in the docker registry the CI does not need
to built a new dependency image.

If the CI detects changes, we have a Job Matrix that creates jobs for all desired triplet+platform combinations. These
Jobs will build the `branch-specific` base, dependency, and development images. A `branch-specific` will be tagged with
the PRs branch name. E.g., the development image of PR 195 on x64 is called
`nebulastream/nes-development:176-Dependency-x64`.

Building the `dependency` image is very expensive; thus, we enable caching in many places to prevent unnecessary
rebuilds of the docker images.

Only build a new set of Docker images if changes to the dependency are detected. Since we rely on a fixed baseline of
vcpkg, changes to the dependencies are always explicit via the vcpkg manifest or the Docker images.
Docker image caching: Every docker image built in the CI builds an additional cache image (suffixed with the
`—cache:tag`). This prevents PRs from rerunning the CI to rebuild its docker images unnecessarily, as all docker builds
have been cached in a previous CI run.
Latest image caching: When the CI builds a new docker image, we also pass the cache image of the `latest` image. Thus,
we could prevent rebuilds in the first invocation of the CI if only changes to the development were made.

### Multiplatform Images

We use multiplatform images that support both x64 and arm64. We do not use `buildx` for cross-compilation; our arm-based
images are built on an arm CPU. Each job in the job matrix produces a platform-specific image (we append -x64 to the
tag). After all matrix jobs have finished, a final image job combines the manifests into a single multiplatform image:
`nebulastream/nes-development:176-Dependency`.

### CI Runner

All jobs requiring a CMake configuration use either the `branch-specific` development image or the `latest` development
image to run the build or all our checks.

# Current State of Dependencies

The current state of dependencies can be observed via the vcpkg.json. Additional dependencies or patches which are
currently
not available via vcpkg are maintained in the `vcpkg/vcpkg-registry/ports` directory.

## Custom Ports

### Folly

We are using a stripped down version of the folly library. We are only using the `MPMCQueues` and `Synchronized`, the
patch throws all transient dependencies which would otherwise be introduced via folly.

### LLVM

Based on the current vcpkg version. The LLVM patch simply disables all default features. This is not possible via
`vcpkg.json` because llvm is included via
nautilus.

### Nautilus

Nautilus is not currently on vcpkg.
