
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(ArithmeticOperations)
add_subdirectory(LogicalOperations)
add_subdirectory(Loop)

add_source_files(unikernel-export
        Operation.cpp
        LoadOperation.cpp
        StoreOperation.cpp
        ConstIntOperation.cpp
        ConstFloatOperation.cpp
        AddressOperation.cpp
        FunctionOperation.cpp
        IfOperation.cpp
        ProxyCallOperation.cpp
        ReturnOperation.cpp
        CastOperation.cpp
        BranchOperation.cpp
        ConstBooleanOperation.cpp
)
add_source_files(nes-runtime
        Operation.cpp
        LoadOperation.cpp
        StoreOperation.cpp
        ConstIntOperation.cpp
        ConstFloatOperation.cpp
        AddressOperation.cpp
        FunctionOperation.cpp
        IfOperation.cpp
        ProxyCallOperation.cpp
        ReturnOperation.cpp
        CastOperation.cpp
        BranchOperation.cpp
        ConstBooleanOperation.cpp
        )
