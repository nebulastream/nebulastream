# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if (NOT NES_DOCKER_AVAILABLE)
    message(STATUS "Docker daemon not reachable; Docker image targets are disabled")
    return()
endif ()

# Custom targets are not part of ALL, so Docker images are built only when requested.
add_custom_target(package-docker-images-all)
add_custom_target(package-docker-runtime-base
    COMMAND ${DOCKER_EXECUTABLE} build --pull=false --load
        --tag "${NES_RUNTIME_BASE_IMAGE}"
        --file "${CMAKE_SOURCE_DIR}/docker/runtime/RuntimeBase.dockerfile"
        "${CMAKE_SOURCE_DIR}/docker/runtime"
    VERBATIM
    USES_TERMINAL
    COMMENT "Building ${NES_RUNTIME_BASE_IMAGE}"
)
add_dependencies(package-docker-images-all package-docker-runtime-base)

function(nes_add_docker_image IMAGE TARGET)
    if (IMAGE STREQUAL "nes-cli")
        set(STATE "RUN install -d -o nes -g nes /state\nVOLUME /state\nENV XDG_STATE_HOME=/state\n")
    else ()
        set(STATE "")
    endif ()

    set(DOCKERFILE "$<TARGET_FILE_DIR:${TARGET}>/${TARGET}.dockerfile")
    string(CONCAT DOCKERFILE_CONTENT
        "FROM ${NES_RUNTIME_BASE_IMAGE}\n"
        # Create the unprivileged runtime user in this per-image layer, not the runtime base:
        # the base is part of the dependency-image hash, so editing it forces a from-scratch
        # rebuild and diverges from the pre-built upstream image. uid/gid pinned so volume
        # ownership is stable across arches; numeric USER satisfies Kubernetes runAsNonRoot.
        "RUN groupadd --gid 10000 nes && useradd --uid 10000 --gid 10000 --create-home nes\n"
        "${STATE}"
        "COPY $<TARGET_FILE_NAME:${TARGET}> /usr/bin/$<TARGET_FILE_NAME:${TARGET}>\n"
        "USER 10000:10000\n"
        "WORKDIR /home/nes\n"
        "ENTRYPOINT [\"/usr/bin/$<TARGET_FILE_NAME:${TARGET}>\"]\n"
    )
    file(GENERATE OUTPUT "${DOCKERFILE}" CONTENT "${DOCKERFILE_CONTENT}")
    file(GENERATE OUTPUT "${DOCKERFILE}.dockerignore"
        CONTENT "*\n!$<TARGET_FILE_NAME:${TARGET}>\n")

    add_custom_target(package-docker-${IMAGE}
        COMMAND /bin/sh -c
            "exec \"$1\" build --pull=false --tag \"$2:\${NES_DOCKER_TAG:-local}\" --file \"$3\" \"$4\""
            _ "${DOCKER_EXECUTABLE}" "nebulastream/${IMAGE}" "${DOCKERFILE}" "$<TARGET_FILE_DIR:${TARGET}>"
        VERBATIM
        USES_TERMINAL
        COMMENT "Building nebulastream/${IMAGE} (NES_DOCKER_TAG, default: local)"
    )
    add_dependencies(package-docker-${IMAGE} ${TARGET} package-docker-runtime-base)
    add_dependencies(package-docker-images-all package-docker-${IMAGE})
endfunction()

nes_add_docker_image(nes-cli nes-cli)
nes_add_docker_image(nes-repl nes-repl)
nes_add_docker_image(nes-repl-embedded nes-repl-embedded)
nes_add_docker_image(nes-worker nes-single-node-worker)
