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
#include <Execution/Operators/Relational/Sort/BatchSortEncode.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

BatchSort::BatchSort(uint64_t operatorHandlerIndex,
                     const std::vector<PhysicalTypePtr>& dataTypes,
                     const std::vector<Record::RecordFieldIdentifier>& fieldIdentifiers,
                     const std::vector<Record::RecordFieldIdentifier>& sortFieldIdentifiers,
                     SortOrder sortOrder)
    : operatorHandlerIndex(operatorHandlerIndex), dataTypes(dataTypes), fieldIdentifiers(fieldIdentifiers),
      sortFieldIdentifiers(sortFieldIdentifiers), sortOrder(sortOrder) {
    NES_ASSERT(dataTypes.size() == fieldIdentifiers.size(), "Data types and field identifiers must have the same size");
    for (const auto& sortFieldIdentifier : sortFieldIdentifiers) {
        auto it = std::find(fieldIdentifiers.begin(), fieldIdentifiers.end(), sortFieldIdentifier);
        NES_ASSERT(it != fieldIdentifiers.end(), "Sort field not found in field identifiers");
    }
}

void* getStackProxy(void* op) {
    NES_ASSERT(op != nullptr, "Operator handler must not be null");
    auto handler = static_cast<BatchSortOperatorHandler*>(op);
    return handler->getState();
}

uint64_t getStateEntrySizeProxy(void* op) {
    NES_ASSERT(op != nullptr, "Operator handler must not be null");
    auto handler = static_cast<BatchSortOperatorHandler*>(op);
    return handler->getStateEntrySize();
}

void setupHandlerProxy(void* op, void* ctx) {
    NES_ASSERT(op != nullptr, "Operator handler must not be null");
    NES_ASSERT(ctx != nullptr, "Context must not be null");
    auto handler = static_cast<BatchSortOperatorHandler*>(op);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext);
}

void BatchSort::setup(ExecutionContext& ctx) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("setupHandlerProxy", setupHandlerProxy, globalOperatorHandler, ctx.getPipelineContext());
}

Value<> encodeData(const Value<>& value, const Value<>& sortOrder) {
    if (value->isType<Boolean>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<bool>, value.as<Boolean>(), sortOrder.as<Boolean>());
    } else if (value->isType<Int8>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<int8_t>, value.as<Int8>(), sortOrder.as<Boolean>());
    } else if (value->isType<Int16>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<int16_t>, value.as<Int16>(), sortOrder.as<Boolean>());
    } else if (value->isType<Int32>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<int32_t>, value.as<Int32>(), sortOrder.as<Boolean>());
    } else if (value->isType<Int64>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<int64_t>, value.as<Int64>(), sortOrder.as<Boolean>());
    } else if (value->isType<UInt8>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<uint8_t>, value.as<UInt8>(), sortOrder.as<Boolean>());
    } else if (value->isType<UInt16>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<uint16_t>, value.as<UInt16>(), sortOrder.as<Boolean>());
    } else if (value->isType<UInt32>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<uint32_t>, value.as<UInt32>(), sortOrder.as<Boolean>());
    } else if (value->isType<UInt64>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<uint64_t>, value.as<UInt64>(), sortOrder.as<Boolean>());
    } else if (value->isType<Float>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<float>, value.as<Float>(), sortOrder.as<Boolean>());
    } else if (value->isType<Double>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<double>, value.as<Double>(), sortOrder.as<Boolean>());
    } else {
        throw Exceptions::NotImplementedException("encodeData is not implemented for the given type.");
    }
}

void BatchSort::execute(ExecutionContext& ctx, Record& record) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto state = Nautilus::FunctionCall("getStackProxy", getStackProxy, globalOperatorHandler);

    auto entrySize = Nautilus::FunctionCall("getStateEntrySizeProxy", getStateEntrySizeProxy, globalOperatorHandler);
    auto vector = Interface::PagedVectorRef(state, entrySize->getValue());

    // create entry and store it in state
    auto entry = vector.allocateEntry();
    // first store sort fields encoded for radix sort
    for (uint64_t i = 0; i < fieldIdentifiers.size(); ++i) {
        for (uint64_t j = 0; j < sortFieldIdentifiers.size(); ++j) {
            if (fieldIdentifiers[i] == sortFieldIdentifiers[j]) {
                auto val = record.read(fieldIdentifiers[i]);
                val = encodeData(val, Value<Boolean>(sortOrder == SortOrder::Descending));
                entry.store(val);
                entry = entry + dataTypes[i]->size();
            }
        }
    }
    // store all fields after the encoded sort fields
    for (uint64_t i = 0; i < fieldIdentifiers.size(); ++i) {
        auto val = record.read(fieldIdentifiers[i]);
        entry.store(val);
        entry = entry + dataTypes[i]->size();
    }
}
}// namespace NES::Runtime::Execution::Operators