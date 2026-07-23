# Packaging binaries as Docker images

The `package-docker-images` CMake target packages the NebulaStream binaries into
slim Docker runtime images based on `nebulastream/nes-runtime-base`. All images
are built from the single parametrized `Package.runtime.dockerfile`, which
replaced the four per-binary dockerfiles that used to live in `docker/frontend/`
and `docker/single-node-worker/`.

## Targets

| Target                         | Image                            | Binary                   |
|--------------------------------|----------------------------------|--------------------------|
| `package-docker-cli`           | `nebulastream/nes-cli`           | `nes-cli`                |
| `package-docker-repl`          | `nebulastream/nes-repl`          | `nes-repl`               |
| `package-docker-embedded-repl` | `nebulastream/nes-repl-embedded` | `nes-repl-embedded`      |
| `package-docker-worker`        | `nebulastream/worker`            | `nes-single-node-worker` |
| `package-docker-images`        | all of the above                 | all four                 |

The image names match what CI pushes to Docker Hub.

## Modes

The `NES_DOCKER_BUILD_IN_DOCKER` environment variable (read at CMake
**configure** time) selects how the binary gets into the image:

* **unset / `0` (default): copy prebuilt.** Each sub-target depends on its
  binary target, (re)builds it in the current build tree, and COPYs it into the
  runtime image. Fast for local iteration, but the binary must be ABI compatible
  with the runtime base image (see caveat below).
* **`1`: rebuild inside Docker.** The docker build compiles the binary inside
  `nebulastream/nes-development` (with a persistent ccache cache mount, build
  context = repository root) and copies the result into the runtime image.
  Nothing is compiled in the local build tree. Slower, but always ABI safe —
  this is what CI uses.

## Usage

```shell
# Package all four binaries (image tag defaults to the git short SHA, or "latest")
cmake --build <build-dir> --target package-docker-images

# Package a single binary
cmake --build <build-dir> --target package-docker-worker

# Override the image tag (resolved at build time, no reconfigure needed)
NES_DOCKER_TAG=my-tag cmake --build <build-dir> --target package-docker-images

# Override the runtime base image tag independently (defaults to "latest")
NES_RUNTIME_TAG=latest cmake --build <build-dir> --target package-docker-worker

# Rebuild inside Docker instead of copying the local binaries
NES_DOCKER_BUILD_IN_DOCKER=1 cmake -B <build-dir> ...
NES_DEV_TAG=latest cmake --build <build-dir> --target package-docker-images
```

## CI

`.github/workflows/create_release_image.yml` (used by the on-demand and nightly
docker image workflows) builds the published executable images through this
target: it configures with `NES_DOCKER_BUILD_IN_DOCKER=1` inside the development
container and runs `cmake --build build -j 1 --target package-docker-images`, so
the compilation happens inside the docker build with its ccache cache mount.

## Gating

The targets are only registered when a Docker daemon is reachable at CMake
configure time (mirroring the docker-based e2e test check). When Docker is
unavailable the targets are not defined and configure prints a hint.

## Caveat: host-libc mismatch (prebuilt mode)

Because the binaries are copied rather than rebuilt, they must be ABI compatible
with the runtime base image (`libc` / `libc++` versions). This is guaranteed when
the binaries are produced with the Docker-based toolchain
(`nebulastream/nes-development`). Binaries built directly on a developer host may
fail to start inside the image due to libc/libc++ version mismatches; in that
case, build inside the development container or use
`NES_DOCKER_BUILD_IN_DOCKER=1`.
