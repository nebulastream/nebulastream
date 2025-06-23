# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


add_plugin("ImageManipToBase64" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)
add_plugin("ImageManipFromBase64" LogicalFunction nes-logical-operators NodeFunctionImageManip.cpp)

add_plugin_as_library("ImageManipToBase64" PhysicalFunction nes-physical-operators image-manip-functions ExecutableFunctionImageManip.cpp)
add_plugin("ImageManipFromBase64" PhysicalFunction image-manip-functions ExecutableFunctionImageManip.cpp)

find_package(OpenSSL CONFIG REQUIRED)
target_link_libraries(image-manip-functions PRIVATE OpenSSL::SSL)
