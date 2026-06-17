# Packaging prebuilt binaries as Docker images

The `package-docker-images` CMake target packages the already-built NebulaStream
binaries into slim Docker runtime images.

Unlike the dockerfiles in `docker/frontend/` and `docker/single-node-worker/`
(which rebuild from source inside Docker), this target **copies** the binaries
produced by the current build tree into the runtime base image
(`nebulastream/nes-runtime-base`). It does not rebuild anything.

## Targets

| Target                      | Image                              | Binary                   |
|-----------------------------|------------------------------------|--------------------------|
| `package-docker-cli`        | `nebulastream/cli`                 | `nes-cli`                |
| `package-docker-repl`       | `nebulastream/repl`                | `nes-repl`               |
| `package-docker-embedded-repl` | `nebulastream/embedded-repl`    | `nes-repl-embedded`      |
| `package-docker-worker`     | `nebulastream/worker`              | `nes-single-node-worker` |
| `package-docker-images`     | all of the above                   | all four                 |

Each sub-target depends on its binary target, so the binary is (re)built first.

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
```

## Gating

The targets are only registered when a Docker daemon is reachable at CMake
configure time (mirroring the docker-based e2e test check). When Docker is
unavailable the targets are not defined and configure prints a hint.

## Caveat: host-libc mismatch

Because the binaries are copied rather than rebuilt, they must be ABI compatible
with the runtime base image (`libc` / `libc++` versions). This is guaranteed when
the binaries are produced with the Docker-based toolchain
(`nebulastream/nes-development`). Binaries built directly on a developer host may
fail to start inside the image due to libc/libc++ version mismatches; in that
case, build the binaries inside the development container before packaging.
