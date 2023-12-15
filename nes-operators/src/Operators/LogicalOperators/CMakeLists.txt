
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-operators
        LogicalOperator.cpp
        Operators/LogicalFilterOperator.cpp
        Operators/LogicalProjectionOperator.cpp
        Operators/LogicalMapOperator.cpp
        Operators/LogicalInferModelOperator.cpp
        Operators/LogicalUnionOperator.cpp
        Operators/LogicalOperatorFactory.cpp
        Operators/LogicalBroadcastOperator.cpp
        Operators/LogicalBatchJoinOperator.cpp
        Operators/LogicalRenameSourceOperator.cpp
        LogicalBinaryOperator.cpp
        LogicalUnaryOperator.cpp
        Operators/LogicalOpenCLOperator.cpp
        Operators/LogicalLimitOperator.cpp
        Operators/BatchJoinDescriptor.cpp
)

add_subdirectory(Sinks)
add_subdirectory(Sources)
add_subdirectory(Watermarks)
add_subdirectory(Windows)
add_subdirectory(Network)
add_subdirectory(UDFs)
