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

#include <QueryCompiler/Experimental/Vectorization/KernelCompiler.hpp>

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Experimental/Vectorization/VectorizableOperator.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>

namespace NES::Nautilus::Backends {

std::shared_ptr<Nautilus::Tracing::ExecutionTrace> KernelCompiler::createTraceFromNautilusOperator(const std::shared_ptr<Runtime::Execution::Operators::Operator>& nautilusOperator) {
    auto pipelineExecutionContextRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) nullptr);
    pipelineExecutionContextRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());

    auto workerContextRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) nullptr);
    workerContextRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());

    auto memRef = Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef(0)));
    memRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 2, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto recordBuffer = Runtime::Execution::RecordBuffer(memRef);

    auto schemaSize = Nautilus::Value<Nautilus::UInt64>((uint64_t)0);
    schemaSize.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 3, NES::Nautilus::IR::Types::StampFactory::createUInt64Stamp());

    auto vectorizedOperator = std::dynamic_pointer_cast<Runtime::Execution::Operators::VectorizableOperator>(nautilusOperator);

    return Nautilus::Tracing::traceFunction([&]() {
        auto traceContext = Nautilus::Tracing::TraceContext::get();
        traceContext->addTraceArgument(recordBuffer.getReference().ref);
        traceContext->addTraceArgument(schemaSize.ref);
        auto ctx = Runtime::Execution::ExecutionContext(workerContextRef, pipelineExecutionContextRef);
        vectorizedOperator->execute(ctx, recordBuffer);
    });
}

} // namespace NES::Nautilus::Backends
