#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_SYMBOLICEXECUTIONCONTEXT_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_SYMBOLICEXECUTIONCONTEXT_HPP_
#include <Experimental/Trace/SymbolicExecution/SymbolicExecutionPath.hpp>
#include <Experimental/Trace/Tag.hpp>
#include <functional>
#include <list>
#include <unordered_map>
namespace NES::ExecutionEngine::Experimental::Trace {
class SymbolicExecutionPath;
class ExecutionTrace;
class ValueRef;

/**
 * @brief The symbolic execution context supports the symbolic execution of functions.
 * In general it executes a function with dummy parameters and explores all possible execution paths.
 */
class SymbolicExecutionContext {
  public:
    // The number of iterations we want to spend maximally to explore executions.
    static const uint64_t MAX_ITERATIONS = 100000;
    SymbolicExecutionContext();
    /**
     * @brief Invokes the symbolic execution of the given function
     * @param function
     * @return The collected trace
     */
    std::shared_ptr<ExecutionTrace> apply(const std::function<void()>& function);

    /**
     * @brief Performs a symbolic execution of a CMP operation.
     * Depending on all previous executions this function determines if a branch should be explored or not.
     * @param valRef
     * @return the return value of this branch
     */
    bool executeCMP(ValueRef& valRef);
  private:
    /**
     * @brief Symbolic execution mode.
     * That identifies if, we follow a previously recorded execution or if we record a new one.
     */
    enum MODE { FOLLOW, RECORD };
    std::unordered_map<Tag, bool, Tag::TagHasher> tagMap;
    TagAddress startAddress;
    std::list<std::shared_ptr<SymbolicExecutionPath>> inflightExecutionPaths;
    MODE currentMode;
    std::shared_ptr<SymbolicExecutionPath> currentExecutionPath;
    uint64_t currentOperation;
};

/**
 * @brief Returns the current symbolic execution context.
 * The symbolic execution context is always thread local.
 * @return SymbolicExecutionContext
 */
SymbolicExecutionContext* getThreadLocalSymbolicExecutionContext();
SymbolicExecutionContext* initThreadSymbolicExecutionContext();
/**
 * @brief Indicates if the symbolic execution is active.
 * @return true if execution is symbolic
 */
bool isInSymbolicExecution();

void initThreadLocalTraceContext();

/**
 * @brief Performs a symbolic execution for the given function
 * @param func
 * @return
 */
template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunctionSymbolically(const Functor func) {
    auto symbolicExecution = initThreadSymbolicExecutionContext();
    return symbolicExecution->apply([&func] {
        func();
    });
}

}// namespace NES::ExecutionEngine::Experimental::Trace

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_SYMBOLICEXECUTIONCONTEXT_HPP_
