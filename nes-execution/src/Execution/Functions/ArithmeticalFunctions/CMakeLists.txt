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
<<<<<<<< HEAD:nes-execution/src/Execution/Functions/ArithmeticalFunctions/CMakeLists.txt
        ExecutableFunctionAdd.cpp
        ExecutableFunctionMul.cpp
        ExecutableFunctionDiv.cpp
        ExecutableFunctionSub.cpp
        )
========
        ReadFieldFunction.cpp
        WriteFieldFunction.cpp
        ConstantValueFunction.cpp
        )

add_subdirectory(ArithmeticalFunctions)
add_subdirectory(LogicalFunctions)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-execution/src/Execution/Functions/CMakeLists.txt
