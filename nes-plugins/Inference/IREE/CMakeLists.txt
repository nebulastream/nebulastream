# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)

add_plugin(ArgMax LogicalFunction nes-logical-operators ArgMaxLogicalFunction.cpp)
add_plugin(ArgMax PhysicalFunction nes-physical-operators ArgMaxExecutableFunction.cpp)

add_plugin(MaxValue LogicalFunction nes-logical-operators MaxValueLogicalFunction.cpp)
add_plugin(MaxValue PhysicalFunction nes-physical-operators MaxValueExecutableFunction.cpp)

add_plugin(iris_label LogicalFunction nes-logical-operators IrisLabelLogicalFunction.cpp)
add_plugin(iris_label PhysicalFunction nes-physical-operators IrisLabelExecutableFunction.cpp)

add_plugin(SpeechRecognitionLabel LogicalFunction nes-logical-operators SpeechRecognitionLabelLogicalFunction.cpp)
add_plugin(SpeechRecognitionLabel PhysicalFunction nes-physical-operators SpeechRecognitionLabelExecutableFunction.cpp)
