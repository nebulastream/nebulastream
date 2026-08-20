# Packaging Docker Images

The NebulaStream executables are packaged into slim runtime images, one CMake target per executable plus
`package-docker-images-all`, which builds all of them:

| target                            | image                                             | binary                   | base    |
|-----------------------------------|---------------------------------------------------|--------------------------|---------|
| `package-docker-runtime-base`     | `nebulastream/nes-runtime-base:<dependency-hash>` | —                        | —       |
| `package-docker-runtime-iree`     | `nebulastream/nes-runtime-iree:<dependency-hash>` | —                        | base    |
| `package-docker-nes-cli`          | `nebulastream/nes-cli`                            | `nes-cli`                | base    |
| `package-docker-nes-repl`         | `nebulastream/nes-repl`                           | `nes-repl`               | base    |
| `package-docker-nes-repl-embedded`| `nebulastream/nes-repl-embedded`                  | `nes-repl-embedded`      | IREE    |
| `package-docker-nes-worker`       | `nebulastream/nes-worker`                         | `nes-single-node-worker` | IREE    |

Every image target is named after the image it builds.

```shell
cmake -B <build-dir> -S .
cmake --build <build-dir> --target package-docker-images-all -j   # all images
cmake --build <build-dir> --target package-docker-nes-worker -j   # a single image
```

Each executable target builds its CMake executable dependency first, uses its `$<TARGET_FILE>` directory as
the Docker context, and generates a Dockerfile-specific ignore file that includes only that binary. They also
depend on the base image target they are built `FROM`. None of the Docker targets are part of the default
build.

## Two base images

`nes-runtime-base` carries only what every binary needs: libc++/libc++abi and `grpc_health_probe`.

`nes-runtime-iree` adds the IREE compiler toolchain (`iree-compile`, `iree-import-onnx`) on top of it. That
toolchain is the large majority of the image, and only a binary that lowers a query plan itself can invoke
it — `nes-inference` shells out to those two tools from `LowerToPhysicalInferModel`. `nes-cli` and `nes-repl`
link `nes-frontend-lib`, which stops at `nes-single-node-worker-interface`, so they can never reach it and
stay on the slim base. `nes-worker` and `nes-repl-embedded` embed the engine and get the IREE base.

The e2e test containers built by `scripts/testing/distributed_bats_lib.bash` use the IREE base regardless, so
inference-tagged tests keep working.

Both tags are the same dependency hash. `docker/runtime` is part of `hash_dependencies.sh`, so editing either
Dockerfile rotates both tags.

## Tags

Executable-image tags default to `local`. Set `NES_DOCKER_TAG` on the build command to use another tag; no
CMake reconfiguration is needed. The runtime-base tag is the dependency hash already used by the Docker e2e
tests.

```shell
NES_DOCKER_TAG=my-tag cmake --build <build-dir> --target package-docker-images-all -j
```

Docker must be reachable during configuration, otherwise CMake warns and the Docker targets are not
registered. When configuring inside the development container, mount the Docker socket as described in the
[development environment guide](development.md).

> [!IMPORTANT]
> The binaries are copied into the image, not rebuilt, so they must be ABI compatible with the runtime base
> image (glibc, libc++ / libstdc++ versions). Binaries built with the NES development image use the expected
> toolchain.

## CI

The published images are built by `.github/workflows/create_executable_images.yml`, which runs the
`package-docker-images-all` target inside the development container and pushes the results. The nightly and the
on-demand image workflows both call into it, so a local run and a CI run produce the images the same way.
