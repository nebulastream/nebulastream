#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_EXCEPTIONS_INTERPRETEREXCEPTION_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_EXCEPTIONS_INTERPRETEREXCEPTION_HPP_
#include <Exceptions/RuntimeException.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {
/**
 * @brief Represents an wrapper for all interpretation related exceptions.
 */
class InterpreterException : public Exceptions::RuntimeException {
  public:
    explicit InterpreterException(const std::string& message, const std::source_location location = std::source_location::current());
};
}
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_EXCEPTIONS_INTERPRETEREXCEPTION_HPP_
