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
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Tracing {




/**
 * @brief We store the symbolic execution context as a thread local pointer.
 * Consequently, each thread maintains its own context and accesses the execution context only locally.
 * Thus, all accesses are single threaded.
 */
static thread_local std::unique_ptr<SymbolicExecutionContext> threadLocalSymbolicExecutionContext;

/**
 * @brief Initializes the symbolic execution context for this thread.
 * @return SymbolicExecutionContext
 */
SymbolicExecutionContext* initThreadSymbolicExecutionContext() {
    threadLocalSymbolicExecutionContext = std::make_unique<SymbolicExecutionContext>();
    return getThreadLocalSymbolicExecutionContext();
}

/**
 * @brief Checks if the symbolic execution context is initialized.
 * @return boolean
 */
bool isInSymbolicExecution() { return threadLocalSymbolicExecutionContext != nullptr; }
void disableSymbolicExecution() { threadLocalSymbolicExecutionContext = nullptr; }

/**
 * @brief Returns the symbolic execution context.
 * @throws RuntimeException if the symbolic execution context is not initialized.
 * @return SymbolicExecutionContext
 */
SymbolicExecutionContext* getThreadLocalSymbolicExecutionContext() { return threadLocalSymbolicExecutionContext.get(); }

SymbolicExecutionContext::SymbolicExecutionContext() { startAddress = Tag::createCurrentAddress(); }

bool SymbolicExecutionContext::executeCMP() {
    auto tag = Tag::createTag(startAddress);
    bool result = true;
    if (currentMode == FOLLOW) {
        auto operation = currentExecutionPath->operator[](currentOperation);
        if (currentOperation == currentExecutionPath->getSize() - 1) {
            bool outcome = get<0>(operation);
            currentMode = RECORD;
            result = !outcome;
            if (!tagMap.contains(tag)) {
                currentExecutionPath->getPath().pop_back();
                currentExecutionPath->append(result, tag);
            }
        } else {
            result = get<0>(operation);
        }
        currentOperation++;
    } else {
        if (tagMap.contains(tag)) {
            result = !tagMap[tag];
        } else {
            tagMap[tag] = result;
        }
        currentExecutionPath->append(result, tag);
        auto subPath = std::make_shared<SymbolicExecutionPath>(*currentExecutionPath);
        inflightExecutionPaths.emplace_back(subPath);
    }
    return result;
}

/**
 * @brief This function implements the symbolic execution.
 * On a high-level it executes the received function till it explored all possible executions.
 * The number of executions is determined by the amount of control-flow splits in the functions.
 * Control-flow splits are caused by branches and loops (loops are basically also branches).
 * For each control-flow split, the symbolic executor have to explore both branches.
 * Thus a function with n branches can lead to 2*n executions.
 * @param function that will be evaluated
 * @return ExecutionTrace the collected trace
 */
std::shared_ptr<ExecutionTrace>
SymbolicExecutionContext::apply(const std::function<NES::Nautilus::Tracing::ValueRef()>& function) {
    // initialize trace context
    initThreadLocalTraceContext();
    auto tracCtx = getThreadLocalTraceContext();
    // initialize symbolic execution context
    auto symExCtx = getThreadLocalSymbolicExecutionContext();
    // as we have not explored the function before we start in record mode with an empty execution path.
    symExCtx->currentMode = SymbolicExecutionContext::RECORD;
    symExCtx->currentExecutionPath = std::make_shared<SymbolicExecutionPath>();
    // evaluate the function for the first time
    auto resultRef = function();
    TraceOperation result = TraceOperation(RETURN);
    if (!resultRef.type->isVoid()) {
        auto intV = cast<NES::Nautilus::IR::Types::IntegerStamp>(resultRef.type);
        result.input.emplace_back(resultRef);
        result.result = resultRef;
    } else {
        result.result = resultRef;
    }
    tracCtx->trace(result);
    uint64_t iterations = 1;
    // for each control-flow split in the function we will have recorded an execution path in inflightExecutionPaths.
    // in the following we will explore all remaining control-flow splits.
    // as each function evaluation can cause another execution path this loop processes as long inflightExecutionPaths has elements.
    while (symExCtx->inflightExecutionPaths.size() > 0) {
        // get the next trace and start in follow mode as we first want to follow the execution till we reach the target control-flow split.
        auto trace = symExCtx->inflightExecutionPaths.front();
        symExCtx->inflightExecutionPaths.pop_front();
        symExCtx->currentMode = SymbolicExecutionContext::FOLLOW;
        symExCtx->currentExecutionPath = trace;
        symExCtx->currentOperation = 0;
        tracCtx->reset();
        // evaluate function
        auto resultRef = function();
        TraceOperation result = TraceOperation(RETURN);
        if (!resultRef.type->isVoid()) {
            result.input.emplace_back(resultRef);
            result.result = resultRef;
        } else {
            result.result = resultRef;
        }
        tracCtx->trace(result);
        iterations++;
        if (iterations > MAX_ITERATIONS) {
            NES_THROW_RUNTIME_ERROR("Symbolic execution caused more than MAX_ITERATIONS iterations. "
                                    "This potentially indicates a bug in the evaluator or the use of recursion in the function.");
        }
    }
    NES_INFO("Symbolic Execution: iterations " << iterations);
    std::cout << tracCtx->getExecutionTrace() << std::endl;

    disableThreadLocalTraceContext();
    disableSymbolicExecution();
    return tracCtx->getExecutionTrace();
}

}// namespace NES::Nautilus::Tracing