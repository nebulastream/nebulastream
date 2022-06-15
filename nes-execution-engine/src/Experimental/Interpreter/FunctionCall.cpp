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

#include <Experimental/Interpreter/FunctionCall.hpp>
#include <execinfo.h>
#include <regex>

namespace NES::ExecutionEngine::Experimental::Interpreter {
std::string getMangledName(void* funcPtr) {
    void *funptr = &funcPtr;
    auto result = backtrace_symbols(&funptr, 6);
    std::string backtraceResult4 = (result[0]);
    std::string backtraceResult5 = (result[1]);
    std::string backtraceResult = (result[2]);
    std::string backtraceResult1 = (result[3]);
    std::string backtraceResult2 = (result[4]);
    std::string backtraceResult3 = (result[5]);
    // we use a regular expression to extract mangled name from backtrace symbol:
    // ...execution-engine-function-execution-test(_Z3addll+0) [0x2c8490]
    std::regex manglednameRegex("(_.+(?=\\+.\\)))");
    auto matchBegin = std::sregex_iterator(backtraceResult.begin(), backtraceResult.end(), manglednameRegex);
    auto matchEnd = std::sregex_iterator();
    for (std::sregex_iterator i = matchBegin; i != matchEnd; ++i) {
        std::smatch match = *i;
        return match.str();
    }

    return "undefinedMangledName";
}
}// namespace NES::ExecutionEngine::Experimental::Interpreter