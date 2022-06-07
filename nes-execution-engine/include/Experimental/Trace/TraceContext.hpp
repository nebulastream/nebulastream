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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACER_HPP_
#include "Operation.hpp"
#include <Experimental/Trace/OpCode.hpp>
#include <Experimental/Trace/Tag.hpp>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <variant>
#include <vector>
namespace NES::ExecutionEngine::Experimental::Trace {

class ExecutionTrace;
class Operation;
class OperationRef;
class Tag;

class TraceContext {
  public:
    TraceContext();

    void reset();
    // void trace(OpCode op, const Value& left, const Value& right, Value& result);
    // void trace(OpCode op, const Value& input, Value& result);
    void trace(Operation& operation);
    void traceCMP(const ValueRef& valueRef, bool result);
    bool isExpectedOperation(OpCode operation);
    std::shared_ptr<OperationRef> isKnownOperation(Tag& tag);
    std::shared_ptr<ExecutionTrace> getExecutionTrace();
    ValueRef createNextRef();

  private:
    uint64_t createStartAddress();
    Operation& getLastOperation();
    std::shared_ptr<ExecutionTrace> executionTrace;
    uint64_t currentOperationCounter = 0;
    TagAddress startAddress;
};

TraceContext* getThreadLocalTraceContext();
void initThreadLocalTraceContext();


template<typename Functor>
std::shared_ptr<ExecutionTrace> traceFunction(Functor func) {
    initThreadLocalTraceContext();
    func();
    Operation result = Operation(RETURN);
    auto tracer = getThreadLocalTraceContext();
    tracer->trace(result);
    return tracer->getExecutionTrace();
}



}// namespace NES::ExecutionEngine::Experimental::Trace

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACER_HPP_
