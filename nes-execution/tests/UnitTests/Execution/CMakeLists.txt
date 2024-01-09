
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


add_subdirectory(Expressions)
add_subdirectory(Operators)
add_subdirectory(Aggregation)
add_subdirectory(PipelineExecution)

if(NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR_INLINING)
    add_nes_execution_test(nautilus-proxy-inlining-test "ProxyInliningCompilationTest.cpp")
endif ()