#include <Exceptions/NotImplementedException.hpp>
#include <Util/StacktraceLoader.hpp>

namespace NES::Exceptions {

NotImplementedException::NotImplementedException(std::string msg, const std::source_location location)
    : RuntimeException(msg, collectAndPrintStacktrace(), location) {}

}// namespace NES::Exceptions