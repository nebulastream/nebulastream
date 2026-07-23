# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# PackageDockerImages.cmake
# -------------------------
# Defines a `package-docker-images` target (plus per-binary `package-docker-<name>`
# sub-targets) that packages NebulaStream binaries into slim Docker runtime images
# (docker/package/Package.runtime.dockerfile). Two modes, selected via the
# NES_DOCKER_BUILD_IN_DOCKER environment variable at CONFIGURE time:
#
#   unset / 0 (default): package the binaries the current build tree produced
#     (via `$<TARGET_FILE:...>`). Each sub-target depends on its binary target,
#     so the binary is (re)built first. The image content matches exactly what
#     was just built, but the binary must be ABI compatible with the runtime base
#     image (nebulastream/nes-runtime-base). When the binaries are produced with
#     the Docker-based toolchain (nebulastream/nes-development) this always
#     holds; local host builds may hit libc/libc++ version mismatches.
#
#   1: REBUILD the binary inside Docker instead. The docker build compiles the
#     cmake target in nebulastream/nes-development (with a persistent ccache
#     cache mount) and copies the result into the runtime image; nothing is
#     compiled in the local build tree. This is what CI uses
#     (.github/workflows/create_release_image.yml).
#
# Invocation:
#   cmake --build <build-dir> --target package-docker-images
#   cmake --build <build-dir> --target package-docker-worker   # single binary
#
# Tag control (all resolved at BUILD time, no reconfigure needed):
#   NES_DOCKER_TAG   image tag; defaults to the git short SHA, or "latest".
#   NES_RUNTIME_TAG  nebulastream/nes-runtime-base tag (default "latest").
#   NES_DEV_TAG      nebulastream/nes-development tag used by the in-docker
#                    build stage (default "latest").
#
# Gating:
#   The targets are only registered when a Docker daemon is reachable at configure
#   time, mirroring the check used for the docker-based e2e tests
#   (cmake/CheckTestUtilities.cmake). When Docker is unavailable the targets are
#   simply not defined.

# Probe for a working Docker daemon, mirroring CheckTestUtilities.cmake.
execute_process(
    COMMAND docker version
    RESULT_VARIABLE NES_PACKAGE_DOCKER_CHECK
    OUTPUT_QUIET
    ERROR_QUIET
)

if (NOT NES_PACKAGE_DOCKER_CHECK EQUAL 0)
    message(STATUS
        "Docker daemon not reachable; 'package-docker-images' target not registered. "
        "Start Docker (or mount the docker socket into the dev container) and re-run "
        "CMake configure to enable it.")
    return()
endif ()

set(NES_PACKAGE_BUILD_IN_DOCKER "$ENV{NES_DOCKER_BUILD_IN_DOCKER}")
if (NES_PACKAGE_BUILD_IN_DOCKER)
    message(STATUS "Docker daemon reachable: 'package-docker-images' target enabled (rebuild inside Docker)")
else ()
    message(STATUS "Docker daemon reachable: 'package-docker-images' target enabled (copy prebuilt binaries)")
endif ()

# Parametrized runtime dockerfile shared by both modes (stage selection via --target).
set(NES_PACKAGE_RUNTIME_DOCKERFILE "${CMAKE_SOURCE_DIR}/docker/package/Package.runtime.dockerfile")

# Map of <sub-target-suffix>=<cmake binary target>=<image name>. The image names
# match what CI pushes to Docker Hub (nebulastream/<image name>).
set(NES_PACKAGE_IMAGES
    "cli=nes-cli=nes-cli"
    "repl=nes-repl=nes-repl"
    "embedded-repl=nes-repl-embedded=nes-repl-embedded"
    "worker=nes-single-node-worker=worker"
)

# Resolve the default tag once at configure time: git short SHA, or "latest".
find_package(Git QUIET)
set(NES_PACKAGE_DEFAULT_TAG "latest")
if (Git_FOUND)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE NES_PACKAGE_GIT_SHA
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE NES_PACKAGE_GIT_RESULT
        ERROR_QUIET
    )
    if (NES_PACKAGE_GIT_RESULT EQUAL 0 AND NOT NES_PACKAGE_GIT_SHA STREQUAL "")
        set(NES_PACKAGE_DEFAULT_TAG "${NES_PACKAGE_GIT_SHA}")
    endif ()
endif ()

# Aggregate target that triggers all per-binary packaging targets.
add_custom_target(package-docker-images
    COMMENT "Packaging all NebulaStream binaries into Docker images")

foreach (NES_PACKAGE_ENTRY ${NES_PACKAGE_IMAGES})
    string(REPLACE "=" ";" NES_PACKAGE_PARTS ${NES_PACKAGE_ENTRY})
    list(GET NES_PACKAGE_PARTS 0 NES_PACKAGE_NAME)
    list(GET NES_PACKAGE_PARTS 1 NES_PACKAGE_TARGET)
    list(GET NES_PACKAGE_PARTS 2 NES_PACKAGE_IMAGE)

    set(NES_PACKAGE_SUBTARGET "package-docker-${NES_PACKAGE_NAME}")

    # NES_DOCKER_TAG / NES_RUNTIME_TAG / NES_DEV_TAG are resolved at BUILD time
    # (not configure time) so a developer can override them per invocation without
    # re-running CMake, e.g. `NES_DOCKER_TAG=my-tag cmake --build ... --target ...`.
    # The default tag is baked in at configure time. The docker build command is a
    # single-line shell script: the CMake/Ninja generators strip the newlines from
    # a bracket argument, so backslash line-continuations would collapse into the
    # next token and break the script. The shell ${VAR:-default} expansions are
    # resolved at build time from the env set by `cmake -E env`. DOCKER_BUILDKIT=1
    # forces BuildKit (cache mounts, stage skipping) even where the docker CLI has
    # no buildx plugin (e.g. the dev container).
    if (NES_PACKAGE_BUILD_IN_DOCKER)
        # Rebuild inside Docker: context = repo root, compile happens in the
        # build-in-docker stage; no dependency on the local binary target.
        add_custom_target(${NES_PACKAGE_SUBTARGET}
            COMMAND ${CMAKE_COMMAND} -E env
                "NES_PACKAGE_DEFAULT_TAG=${NES_PACKAGE_DEFAULT_TAG}"
                "NES_PACKAGE_IMAGE_NAME=nebulastream/${NES_PACKAGE_IMAGE}"
                "NES_PACKAGE_BINARY_TARGET=${NES_PACKAGE_TARGET}"
                "NES_PACKAGE_BIN_NAME=$<TARGET_FILE_NAME:${NES_PACKAGE_TARGET}>"
                "NES_PACKAGE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
                "NES_PACKAGE_DOCKERFILE=${NES_PACKAGE_RUNTIME_DOCKERFILE}"
                "NES_PACKAGE_CONTEXT=${CMAKE_SOURCE_DIR}"
                "DOCKER_BUILDKIT=1"
                sh -c [[docker build --target build-in-docker -t "${NES_PACKAGE_IMAGE_NAME}:${NES_DOCKER_TAG:-$NES_PACKAGE_DEFAULT_TAG}" --build-arg "TAG=${NES_DEV_TAG:-latest}" --build-arg "RUNTIME_TAG=${NES_RUNTIME_TAG:-latest}" --build-arg "BINARY_TARGET=${NES_PACKAGE_BINARY_TARGET}" --build-arg "BIN_NAME=${NES_PACKAGE_BIN_NAME}" --build-arg "BUILD_TYPE=${NES_PACKAGE_BUILD_TYPE:-RelWithDebInfo}" -f "${NES_PACKAGE_DOCKERFILE}" "${NES_PACKAGE_CONTEXT}"]]
            DEPENDS "${NES_PACKAGE_RUNTIME_DOCKERFILE}"
            VERBATIM
            COMMENT "Building ${NES_PACKAGE_TARGET} inside Docker into nebulastream/${NES_PACKAGE_IMAGE} image"
        )
    else ()
        # Per-image staging directory; the prebuilt stage uses this as its (tiny)
        # build context and expects exactly one file: the binary named like the
        # cmake target's output file (i.e. $<TARGET_FILE_NAME:...>).
        set(NES_PACKAGE_STAGING "${CMAKE_BINARY_DIR}/docker-package/${NES_PACKAGE_NAME}")

        add_custom_target(${NES_PACKAGE_SUBTARGET}
            # Recreate a clean staging dir holding only the freshly-built binary.
            COMMAND ${CMAKE_COMMAND} -E rm -rf "${NES_PACKAGE_STAGING}"
            COMMAND ${CMAKE_COMMAND} -E make_directory "${NES_PACKAGE_STAGING}"
            COMMAND ${CMAKE_COMMAND} -E copy
                "$<TARGET_FILE:${NES_PACKAGE_TARGET}>"
                "${NES_PACKAGE_STAGING}/$<TARGET_FILE_NAME:${NES_PACKAGE_TARGET}>"
            COMMAND ${CMAKE_COMMAND} -E env
                "NES_PACKAGE_DEFAULT_TAG=${NES_PACKAGE_DEFAULT_TAG}"
                "NES_PACKAGE_IMAGE_NAME=nebulastream/${NES_PACKAGE_IMAGE}"
                "NES_PACKAGE_BIN_NAME=$<TARGET_FILE_NAME:${NES_PACKAGE_TARGET}>"
                "NES_PACKAGE_DOCKERFILE=${NES_PACKAGE_RUNTIME_DOCKERFILE}"
                "NES_PACKAGE_CONTEXT=${NES_PACKAGE_STAGING}"
                "DOCKER_BUILDKIT=1"
                sh -c [[docker build --target prebuilt -t "${NES_PACKAGE_IMAGE_NAME}:${NES_DOCKER_TAG:-$NES_PACKAGE_DEFAULT_TAG}" --build-arg "RUNTIME_TAG=${NES_RUNTIME_TAG:-latest}" --build-arg "BIN_NAME=${NES_PACKAGE_BIN_NAME}" -f "${NES_PACKAGE_DOCKERFILE}" "${NES_PACKAGE_CONTEXT}"]]
            DEPENDS ${NES_PACKAGE_TARGET} "${NES_PACKAGE_RUNTIME_DOCKERFILE}"
            VERBATIM
            COMMENT "Packaging ${NES_PACKAGE_TARGET} into nebulastream/${NES_PACKAGE_IMAGE} Docker image"
        )
    endif ()

    add_dependencies(package-docker-images ${NES_PACKAGE_SUBTARGET})
endforeach ()
