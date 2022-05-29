#include <Experimental/Interpreter/Trace/FunctionCallTarget.hpp>
#include <string>

namespace NES::Interpreter{

FunctionCallTarget::FunctionCallTarget(const std::__cxx11::basic_string<char>& functionName,
                                       const std::__cxx11::basic_string<char>& mangledName)
    : functionName(functionName), mangledName(mangledName) {}
std::ostream& operator<<(std::ostream& os, const FunctionCallTarget& target) {
    os << target.functionName;
    return os;
}
}