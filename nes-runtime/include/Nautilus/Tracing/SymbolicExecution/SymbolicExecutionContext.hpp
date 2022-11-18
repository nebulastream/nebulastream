/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_SYMBOLICEXECUTIONCONTEXT_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_SYMBOLICEXECUTIONCONTEXT_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionPath.hpp>
#include <Nautilus/Tracing/Tag.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <functional>
#include <list>
#include <unordered_map>
namespace NES::Nautilus::Tracing {
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
    std::shared_ptr<ExecutionTrace> apply(const std::function<NES::Nautilus::Tracing::ValueRef()>& function);

    /**
     * @brief Performs a symbolic execution of a CMP operation.
     * Depending on all previous executions this function determines if a branch should be explored or not.
     * @return the return value of this branch
     */
    bool executeCMP();

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
void disableSymbolicExecution();
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
    auto result = symbolicExecution->apply([&func] {
        func();
        return createNextRef(Nautilus::IR::Types::StampFactory::createVoidStamp());
    });
    disableSymbolicExecution();
    return result;
}

template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunctionSymbolicallyWithReturn(const Functor func) {
    auto symbolicExecution = initThreadSymbolicExecutionContext();
    return symbolicExecution->apply([&func] {
        auto res = func();
        return res.ref;
    });
}

}// namespace NES::Nautilus::Tracing

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_TRACE_SYMBOLICEXECUTIONCONTEXT_HPP_
