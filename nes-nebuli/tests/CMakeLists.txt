# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Register all REPL tests using pytest discovery
add_test(
    NAME "repl-tests"
    COMMAND python3 -m pytest ${CMAKE_CURRENT_SOURCE_DIR}/test_repl_pytest.py -v
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
set_tests_properties(repl-tests PROPERTIES
    ENVIRONMENT "CMAKE_BINARY_DIR=${CMAKE_BINARY_DIR}"
)

