#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_FUNCTIONCALLTARGET_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_FUNCTIONCALLTARGET_HPP_
#include <ostream>
#include <string>
namespace NES::Interpreter {

class FunctionCallTarget {
  public:
    FunctionCallTarget(const std::string& functionName, const std::string& mangledName);
    std::string functionName;
    std::string mangledName;
    friend std::ostream& operator<<(std::ostream& os, const FunctionCallTarget& target);
};

}

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_FUNCTIONCALLTARGET_HPP_
