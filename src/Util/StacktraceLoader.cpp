#include <NodeEngine/internal/backtrace.hpp>
#include <Util/Logger.hpp>
#include <Util/StacktraceLoader.hpp>

#define CALLSTACK_MAX_SIZE 32

namespace NES {
/**
 * @brief This methods collects the call stacks and prints is
 */
void collectAndPrintStacktrace() {
    backward::StackTrace stackTrace;
    backward::Printer printer;
    stackTrace.load_here(CALLSTACK_MAX_SIZE);
    std::stringbuf buffer;
    std::ostream os(&buffer);
    printer.print(stackTrace, os);
    NES_ERROR("Stacktrace:\n " << buffer.str());
}
}// namespace NES