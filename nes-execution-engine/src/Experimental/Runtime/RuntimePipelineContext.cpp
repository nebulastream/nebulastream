#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Operator.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution {

void RuntimePipelineContext::dispatchBuffer(Runtime::TupleBuffer&) {}

RuntimePipelineContext::OperatorStateTag RuntimePipelineContext::registerGlobalOperatorState(
    std::unique_ptr<ExecutionEngine::Experimental::Interpreter::OperatorState> operatorState) {
    operatorStates.push_back(std::move(operatorState));
    return operatorStates.size() - 1;
}

ExecutionEngine::Experimental::Interpreter::OperatorState*
RuntimePipelineContext::getGlobalOperatorState(RuntimePipelineContext::OperatorStateTag tag) {
    return operatorStates[tag].get();
}

}// namespace NES::Runtime::Execution