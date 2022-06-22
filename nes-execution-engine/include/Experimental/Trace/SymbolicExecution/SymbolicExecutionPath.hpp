#include <Experimental/Trace/Tag.hpp>
#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_SYMBOLICEXECUTION_SYMBOLICEXECUTIONPATH_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_SYMBOLICEXECUTION_SYMBOLICEXECUTIONPATH_HPP_

namespace NES::ExecutionEngine::Experimental::Trace {

/**
 * @brief The SymbolicExecutionPath represents all tags in one specific execution.
 */
class SymbolicExecutionPath {
  public:
    void append(bool outcome, Tag& tag);
    std::tuple<bool, Tag> operator[](uint64_t size);
    uint64_t getSize();
  private:
    std::vector<std::tuple<bool, Tag>> path;
};

}

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_SYMBOLICEXECUTION_SYMBOLICEXECUTIONPATH_HPP_
