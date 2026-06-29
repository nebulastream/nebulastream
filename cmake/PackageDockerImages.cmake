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
# sub-targets) that packages already-built NebulaStream binaries into slim Docker
# runtime images.
#
# IMPORTANT: this does NOT rebuild the binaries inside Docker. It COPIES the
# binaries that the current build tree produced (via `$<TARGET_FILE:...>`) into
# the runtime base image (nebulastream/nes-runtime-base). This guarantees the
# image content matches exactly what was just built, but means the binary must be
# ABI compatible with the runtime base image. When the binaries are produced with
# the Docker-based toolchain (nebulastream/nes-development) this always holds;
# local host builds may hit libc/libc++ version mismatches against the base image.
#
# Invocation:
#   cmake --build <build-dir> --target package-docker-images
#   cmake --build <build-dir> --target package-docker-worker   # single binary
#
# Tag control:
#   The image tag is read from the NES_DOCKER_TAG environment variable at build
#   time. When unset it falls back to the git short SHA, or to "latest" if git is
#   not available. The runtime base image tag can be overridden independently via
#   the NES_RUNTIME_TAG environment variable (defaults to "latest").
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

message(STATUS "Docker daemon reachable: 'package-docker-images' target enabled")

# Runtime dockerfile that COPYs a single prebuilt binary into the runtime base.
set(NES_PACKAGE_RUNTIME_DOCKERFILE "${CMAKE_SOURCE_DIR}/docker/package/Package.runtime.dockerfile")

# Map of <sub-target-suffix> -> <cmake binary target>. The image is named
# nebulastream/<sub-target-suffix>:<tag>.
set(NES_PACKAGE_IMAGES
    "cli=nes-cli"
    "repl=nes-repl"
    "embedded-repl=nes-repl-embedded"
    "worker=nes-single-node-worker"
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

    set(NES_PACKAGE_SUBTARGET "package-docker-${NES_PACKAGE_NAME}")

    # Per-image staging directory; the runtime dockerfile uses this as its (tiny)
    # build context and expects exactly one file: the binary named like the cmake
    # target's output file (i.e. $<TARGET_FILE_NAME:...>).
    set(NES_PACKAGE_STAGING "${CMAKE_BINARY_DIR}/docker-package/${NES_PACKAGE_NAME}")

    # NES_DOCKER_TAG / NES_RUNTIME_TAG are resolved at BUILD time (not configure
    # time) so a developer can override them per invocation without re-running
    # CMake, e.g. `NES_DOCKER_TAG=my-tag cmake --build ... --target ...`.
    # The default tag is baked in at configure time.
    add_custom_target(${NES_PACKAGE_SUBTARGET}
        # Recreate a clean staging dir holding only the freshly-built binary.
        COMMAND ${CMAKE_COMMAND} -E rm -rf "${NES_PACKAGE_STAGING}"
        COMMAND ${CMAKE_COMMAND} -E make_directory "${NES_PACKAGE_STAGING}"
        COMMAND ${CMAKE_COMMAND} -E copy
            "$<TARGET_FILE:${NES_PACKAGE_TARGET}>"
            "${NES_PACKAGE_STAGING}/$<TARGET_FILE_NAME:${NES_PACKAGE_TARGET}>"
        # Build the runtime image. ${NES_DOCKER_TAG:-default} resolves the env var
        # at build time, falling back to the configure-time default tag.
        COMMAND ${CMAKE_COMMAND} -E env
            "NES_PACKAGE_DEFAULT_TAG=${NES_PACKAGE_DEFAULT_TAG}"
            "NES_PACKAGE_IMAGE_NAME=nebulastream/${NES_PACKAGE_NAME}"
            "NES_PACKAGE_BIN_NAME=$<TARGET_FILE_NAME:${NES_PACKAGE_TARGET}>"
            "NES_PACKAGE_DOCKERFILE=${NES_PACKAGE_RUNTIME_DOCKERFILE}"
            "NES_PACKAGE_CONTEXT=${NES_PACKAGE_STAGING}"
            # Single-line shell command: the CMake/Ninja generators strip the
            # newlines from a bracket argument, so backslash line-continuations
            # would collapse into the next token and break the script. Keep it on
            # one logical line. The shell ${VAR:-default} expansions are resolved
            # at build time from the env set by `cmake -E env` above.
            sh -c [[docker build --load -t "${NES_PACKAGE_IMAGE_NAME}:${NES_DOCKER_TAG:-$NES_PACKAGE_DEFAULT_TAG}" --build-arg "RUNTIME_TAG=${NES_RUNTIME_TAG:-latest}" --build-arg "BIN_NAME=${NES_PACKAGE_BIN_NAME}" -f "${NES_PACKAGE_DOCKERFILE}" "${NES_PACKAGE_CONTEXT}"]]
        DEPENDS ${NES_PACKAGE_TARGET} "${NES_PACKAGE_RUNTIME_DOCKERFILE}"
        VERBATIM
        COMMENT "Packaging ${NES_PACKAGE_TARGET} into nebulastream/${NES_PACKAGE_NAME} Docker image"
    )

    add_dependencies(package-docker-images ${NES_PACKAGE_SUBTARGET})
endforeach ()
