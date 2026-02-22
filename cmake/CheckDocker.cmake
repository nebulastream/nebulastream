# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Validate docker is available when ENABLE_DOCKER_TESTS is ON
if (ENABLE_DOCKER_TESTS)
    # Check if docker is available
    execute_process(
        COMMAND docker version
        RESULT_VARIABLE DOCKER_CHECK_RESULT
        OUTPUT_QUIET
        ERROR_QUIET
    )

    if (NOT DOCKER_CHECK_RESULT EQUAL 0)
        message(WARNING
            "ENABLE_DOCKER_TESTS is ON but docker is not working.\n"
            "  For dev container: Mount docker socket with -v /var/run/docker.sock:/var/run/docker.sock\n"
            "  For host system: Ensure docker is installed and running\n"
            "  Set -DENABLE_DOCKER_TESTS=OFF to suppress this warning\n"
            "Docker tests will be automatically disabled."
        )
        set(ENABLE_DOCKER_TESTS OFF PARENT_SCOPE)
    else()
        message(STATUS "Docker tests enabled: using docker")
    endif()
endif()
