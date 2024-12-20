# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-execution
        PhysicalInferModelOperator.cpp
        PhysicalOperator.cpp
        PhysicalUnaryOperator.cpp
        PhysicalBinaryOperator.cpp
        PhysicalSelectionOperator.cpp
        PhysicalLimitOperator.cpp
        PhysicalProjectOperator.cpp
        PhysicalMapOperator.cpp
        PhysicalUnionOperator.cpp
        PhysicalDemultiplexOperator.cpp
        PhysicalWatermarkAssignmentOperator.cpp
        PhysicalScanOperator.cpp
        PhysicalEmitOperator.cpp
        PhysicalSortBufferOperator.cpp
        PhysicalDelayBufferOperator.cpp
        PhysicalDelayTuplesOperator.cpp
)

add_subdirectory(Joining)

add_subdirectory(Windowing)

