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
#include <Execution/Operators/Relational/BatchDistinct.hpp>
#include <Execution/Operators/Relational/BatchDistinctOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <cstdint>
#include <unordered_set>

namespace NES::Runtime::Execution::Operators {

BatchDistinct::BatchDistinct(const uint64_t operatorHandlerIndex, const std::vector<PhysicalTypePtr>& dataTypes)
    : operatorHandlerIndex(operatorHandlerIndex), dataTypes(dataTypes) {}

void* getStackProxyDistinct(void* op) {
    auto handler = static_cast<BatchDistinctOperatorHandler*>(op);
    return handler->getState();
}

uint64_t getStateEntrySizeDistinct(void* op) {
    auto handler = static_cast<BatchDistinctOperatorHandler*>(op);
    return handler->getStateEntrySize();
}

void setupHandlerDistinct(void* op, void* ctx) {
    auto handler = static_cast<BatchDistinctOperatorHandler*>(op);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext);
}

void BatchDistinct::setup(ExecutionContext& ctx) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("setupHandler", setupHandlerDistinct, globalOperatorHandler, ctx.getPipelineContext());
}

void BatchDistinct::execute(ExecutionContext& ctx, Record& record) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto state = Nautilus::FunctionCall("getStackProxy", getStackProxyDistinct, globalOperatorHandler);

    auto entrySize = Nautilus::FunctionCall("getStateEntrySize", getStateEntrySizeDistinct, globalOperatorHandler);
    auto vector = Interface::PagedVectorRef(state, entrySize->getValue());

    // create entry and store it in state
    // Todo: only do so for one field?
    // -> for now yes. We start with DISTINCT on a single field/column/attribute
    // -> we then extend towards multiple fields/columns/attributes
    auto entry = vector.allocateEntry();
    auto fields = record.getAllFields();
    for (size_t i = 0; i < fields.size(); ++i) {
        auto val = record.read(fields[i]);
        // auto test = getValueSet();
        // if(this->valueSet.emplace(val.value->toString()).second) {
        // if(valueSet.emplace(val->staticCast<uint32_t>()).second) {
        // if(valueSet.contains(val.getValue().staticCast<uint32_t>())) {
        if(test == 1) {
            entry.store(val);
            entry = entry + dataTypes[i]->size();
        }
    }
}
}// namespace NES::Runtime::Execution::Operators