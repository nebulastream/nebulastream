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