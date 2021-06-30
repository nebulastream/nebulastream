#include <Compiler/Exceptions/CompilerException.hpp>
#include <Util/Logger.hpp>
#include <NodeEngine/internal/backtrace.hpp>
namespace NES::Compiler {

Compiler::CompilerException::CompilerException(const std::string& message)
    : NesRuntimeException(message, NES::NodeEngine::collectAndPrintStacktrace()) {
    NES_ERROR(message);
}

}// namespace NES::Compiler