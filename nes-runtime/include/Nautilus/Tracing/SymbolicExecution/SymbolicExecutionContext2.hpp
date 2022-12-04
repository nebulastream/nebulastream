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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_SYMBOLICEXECUTION_SYMBOLICEXECUTIONCONTEXT2_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_SYMBOLICEXECUTION_SYMBOLICEXECUTIONCONTEXT2_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionPath.hpp>
#include <Nautilus/Tracing/Tag.hpp>
#include <Nautilus/Tracing/TagRecorder.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <functional>
#include <list>
#include <unordered_set>
namespace NES::Nautilus::Tracing {
class SymbolicExecutionPath;
class ExecutionTrace;
class ValueRef;

/**
 * @brief The symbolic execution context supports the symbolic execution of functions.
 * In general it executes a function with dummy parameters and explores all possible execution paths.
 */
class SymbolicExecutionContext2 {
  public:
    // The number of iterations we want to spend maximally to explore executions.
    static const uint64_t MAX_ITERATIONS = 100000;
    SymbolicExecutionContext2();

    /**
     * @brief Performs a symbolic execution of a CMP operation.
     * Depending on all previous executions this function determines if a branch should be explored or not.
     * @return the return value of this branch
     */
    bool executeCMP(TagRecorder* tr);
    bool shouldContinue();
    void next();

  private:
    bool follow(TagRecorder* tr);
    bool record(TagRecorder* tr);

  private:
    /**
     * @brief Symbolic execution mode.
     * That identifies if, we follow a previously recorded execution or if we record a new one.
     */
    enum MODE : int8_t { FOLLOW, RECORD };
    enum TagState : int8_t { FirstVisit, SecondVisit };
    std::unordered_map<Tag, TagState, Tag::TagHasher> tagMap;
    std::list<SymbolicExecutionPath> inflightExecutionPaths;
    MODE currentMode = RECORD;
    SymbolicExecutionPath currentExecutionPath = SymbolicExecutionPath();
    uint64_t currentOperation = 0;
    uint64_t iterations = 0;
};

}// namespace NES::Nautilus::Tracing

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_SYMBOLICEXECUTION_SYMBOLICEXECUTIONCONTEXT2_HPP_
