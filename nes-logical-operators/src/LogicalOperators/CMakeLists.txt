# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(Sinks)
add_subdirectory(Sources)
add_subdirectory(Windows)

add_source_files(nes-logical-operators
        Operator.cpp
)

add_plugin(Selection Operator nes-logical-operators SelectionOperator.cpp)
add_plugin(EventTimeWatermarkAssigner Operator nes-logical-operators EventTimeWatermarkAssignerOperator.cpp)
add_plugin(IngestionTimeWatermarkAssigner Operator nes-logical-operators IngestionTimeWatermarkAssignerOperator.cpp)
add_plugin(Map Operator nes-logical-operators MapOperator.cpp)
add_plugin(Projection Operator nes-logical-operators ProjectionOperator.cpp)
add_plugin(Union Operator nes-logical-operators UnionOperator.cpp)
