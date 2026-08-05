# Packaging Docker Images

The NebulaStream executables are packaged into slim runtime images based on `nebulastream/nes-runtime-base`,
one CMake target per executable plus `package-docker-images-all`, which builds all of them:

| target                            | image                                             | binary                   |
|-----------------------------------|---------------------------------------------------|--------------------------|
| `package-docker-runtime-base`     | `nebulastream/nes-runtime-base:<dependency-hash>` | —                        |
| `package-docker-nes-cli`          | `nebulastream/nes-cli`                            | `nes-cli`                |
| `package-docker-nes-repl`         | `nebulastream/nes-repl`                           | `nes-repl`               |
| `package-docker-nes-repl-embedded`| `nebulastream/nes-repl-embedded`                  | `nes-repl-embedded`      |
| `package-docker-nes-worker`       | `nebulastream/nes-worker`                         | `nes-single-node-worker` |

Every image target is named after the image it builds.

```shell
cmake -B <build-dir> -S .
cmake --build <build-dir> --target package-docker-images-all -j   # all images
cmake --build <build-dir> --target package-docker-nes-worker -j   # a single image
```

Each executable target builds its CMake executable dependency first, uses its `$<TARGET_FILE>` directory as
the Docker context, and generates a Dockerfile-specific ignore file that includes only that binary. They also
depend on `package-docker-runtime-base`. None of the Docker targets are part of the default build.

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
