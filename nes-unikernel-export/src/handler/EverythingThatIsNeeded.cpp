//
// Created by ls on 04.10.23.
//
#include <Execution/Operators/ExecutionContext.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {
using namespace Nautilus;
uint64_t Runtime::Execution::PipelineExecutionContext::getNumberOfWorkerThreads() const { return 1; }
//void* getGlobalOperatorHandlerProxy(void* pc, uint64_t index) { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

//Value<MemRef> Runtime::Execution::ExecutionContext::getGlobalOperatorHandler(uint64_t handlerIndex) {
//    Value<UInt64> handlerIndexValue = Value<UInt64>(handlerIndex);
//    return FunctionCall<>("getGlobalOperatorHandlerProxy", getGlobalOperatorHandlerProxy, pipelineContext, handlerIndexValue);
//}
}// namespace NES