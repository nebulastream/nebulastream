#include <Experimental/Runtime/PipelineContext.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Operator.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution {

void PipelineContext::dispatchBuffer(Runtime::TupleBuffer&) {}

PipelineContext::OperatorStateTag PipelineContext::registerGlobalOperatorState(
    std::unique_ptr<ExecutionEngine::Experimental::Interpreter::OperatorState> operatorState) {
    operatorStates.push_back(std::move(operatorState));
    return operatorStates.size() - 1;
}

ExecutionEngine::Experimental::Interpreter::OperatorState*
PipelineContext::getGlobalOperatorState(PipelineContext::OperatorStateTag tag) {
    return operatorStates[tag].get();
}

}// namespace NES::Runtime::Execution