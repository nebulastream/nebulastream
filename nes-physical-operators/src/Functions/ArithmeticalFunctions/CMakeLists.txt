# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(Add ExecutableFunction nes-execution ExecutableFunctionAdd.cpp)
add_plugin(Div ExecutableFunction nes-execution ExecutableFunctionDiv.cpp)
add_plugin(Mod ExecutableFunction nes-execution ExecutableFunctionMod.cpp)
add_plugin(Mul ExecutableFunction nes-execution ExecutableFunctionMul.cpp)
add_plugin(Sub ExecutableFunction nes-execution ExecutableFunctionSub.cpp)
