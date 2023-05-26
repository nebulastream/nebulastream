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

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Sort/BatchSort.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Nautilus/Interface/Stack/StackRef.hpp>

namespace NES::Runtime::Execution::Operators {

BatchSort::BatchSort(const uint64_t operatorHandlerIndex, const std::vector<PhysicalTypePtr>& dataTypes)
    : operatorHandlerIndex(operatorHandlerIndex), dataTypes(dataTypes) {}

void* getStackProxy(void* op) {
    auto handler = static_cast<BatchSortOperatorHandler*>(op);
    return handler->getState();
}

uint64_t getStateEntrySize(void* op) {
    auto handler = static_cast<BatchSortOperatorHandler*>(op);
    return handler->getStateEntrySize();
}

void setupHandler(void* op, void* ctx) {
    auto handler = static_cast<BatchSortOperatorHandler*>(op);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext);
}

void BatchSort::setup(ExecutionContext& ctx) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("setupHandler", setupHandler, globalOperatorHandler, ctx.getPipelineContext());
}

void BatchSort::execute(ExecutionContext& ctx, Record& record) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto state = Nautilus::FunctionCall("getStackProxy", getStackProxy, globalOperatorHandler);

    auto entrySize = Nautilus::FunctionCall("getStateEntrySize", getStateEntrySize, globalOperatorHandler);
    auto stack = Interface::StackRef(state, entrySize->getValue());

    // create entry and store it in state
    auto entry = stack.allocateEntry();
    auto fields = record.getAllFields();
    for (size_t i = 0; i < fields.size(); ++i) {
        auto val = record.read(fields[i]);
        entry.store(val);
        entry = entry + dataTypes[i]->size();
    }
}
}// namespace NES::Runtime::Execution::Operators