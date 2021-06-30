#ifndef NES_INCLUDE_COMPILER_EXCEPTIONS_COMPILEREXCEPTION_HPP_
#define NES_INCLUDE_COMPILER_EXCEPTIONS_COMPILEREXCEPTION_HPP_
#include <Exceptions/NesRuntimeException.hpp>
namespace NES::Compiler{

class CompilerException : public NesRuntimeException{
  public:
   explicit CompilerException(const std::string& message);
};

}

#endif//NES_INCLUDE_COMPILER_EXCEPTIONS_COMPILEREXCEPTION_HPP_
