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

add_plugin(iris_label LogicalFunction nes-functions IrisLabelLogicalFunction.cpp)
add_plugin(iris_label ExecutableFunction nes-execution IrisLabelExecutableFunction.cpp)

add_plugin(ArgMax LogicalFunction nes-functions ArgMaxLogicalFunction.cpp)
add_plugin(ArgMax ExecutableFunction nes-execution ArgMaxExecutableFunction.cpp)

add_plugin(MaxValue LogicalFunction nes-functions MaxValueLogicalFunction.cpp)
add_plugin(MaxValue ExecutableFunction nes-execution MaxValueExecutableFunction.cpp)

add_plugin(SpeechRecognitionLabel LogicalFunction nes-functions SpeechRecognitionLabelLogicalFunction.cpp)
add_plugin(SpeechRecognitionLabel ExecutableFunction nes-execution SpeechRecognitionLabelExecutableFunction.cpp)
