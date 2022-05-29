/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Experimental/Interpreter/Trace/FunctionCallTarget.hpp>
#include <string>

namespace NES::Experimental::Interpreter{

FunctionCallTarget::FunctionCallTarget(const std::__cxx11::basic_string<char>& functionName,
                                       const std::__cxx11::basic_string<char>& mangledName)
    : functionName(functionName), mangledName(mangledName) {}
std::ostream& operator<<(std::ostream& os, const FunctionCallTarget& target) {
    os << target.functionName;
    return os;
}
}