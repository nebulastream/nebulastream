#ifndef NES_INCLUDE_QUERYCOMPILER_EXCEPTIONS_QUERYCOMPILATIONEXCEPTION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_EXCEPTIONS_QUERYCOMPILATIONEXCEPTION_HPP_
#include <stdexcept>
#include <Exceptions/NesRuntimeException.hpp>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Exception thrown by the query compiler.
 */
class QueryCompilationException : public NesRuntimeException{
  public:
    QueryCompilationException(std::string message);
};

}
}

#endif//NES_INCLUDE_QUERYCOMPILER_EXCEPTIONS_QUERYCOMPILATIONEXCEPTION_HPP_
