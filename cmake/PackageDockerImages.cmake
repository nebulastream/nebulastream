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

# PLANNER: the image plans queries, so it needs the Calcite planner: the native library, copied next to the binary and
# installed into /usr/lib.
function(nes_add_docker_image IMAGE TARGET)
    cmake_parse_arguments(ARG "PLANNER" "" "" ${ARGN})
    if (IMAGE STREQUAL "nes-cli")
        # The runtime base runs as the unprivileged nes user; the volume must be created as root and owned by nes.
        set(STATE "USER root\nRUN install -d -o nes -g nes /state\nUSER 10000:10000\nVOLUME /state\nENV XDG_STATE_HOME=/state\n")
    else ()
        set(STATE "")
    endif ()

    set(PLANNER "")
    set(PLANNER_IGNORE "")
    set(PLANNER_COPY "")
    if (ARG_PLANNER)
        get_filename_component(PLANNER_LIBRARY "${NES_CALCITE_NATIVE_LIBRARY}" NAME)
        set(PLANNER "COPY ${PLANNER_LIBRARY} /usr/lib/${PLANNER_LIBRARY}\n")
        set(PLANNER_IGNORE "!${PLANNER_LIBRARY}\n")
        set(PLANNER_COPY COMMAND ${CMAKE_COMMAND} -E copy_if_different "${NES_CALCITE_NATIVE_LIBRARY}" "$<TARGET_FILE_DIR:${TARGET}>")
    endif ()

    set(DOCKERFILE "$<TARGET_FILE_DIR:${TARGET}>/${TARGET}.dockerfile")
    string(CONCAT DOCKERFILE_CONTENT
        "FROM ${NES_RUNTIME_BASE_IMAGE}\n"
        "${STATE}"
        "${PLANNER}"
        "COPY $<TARGET_FILE_NAME:${TARGET}> /usr/bin/$<TARGET_FILE_NAME:${TARGET}>\n"
        "ENTRYPOINT [\"/usr/bin/$<TARGET_FILE_NAME:${TARGET}>\"]\n"
    )
    file(GENERATE OUTPUT "${DOCKERFILE}" CONTENT "${DOCKERFILE_CONTENT}")
    file(GENERATE OUTPUT "${DOCKERFILE}.dockerignore"
        CONTENT "*\n!$<TARGET_FILE_NAME:${TARGET}>\n${PLANNER_IGNORE}")

    add_custom_target(package-docker-${IMAGE}
        ${PLANNER_COPY}
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

nes_add_docker_image(nes-cli nes-cli PLANNER)
nes_add_docker_image(nes-repl nes-repl PLANNER)
nes_add_docker_image(nes-repl-embedded nes-repl-embedded PLANNER)
nes_add_docker_image(nes-worker nes-single-node-worker)
