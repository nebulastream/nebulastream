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
#include <Execution/Operators/Relational/Sort/BatchSortEncode.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

BatchSort::BatchSort(uint64_t operatorHandlerIndex,
                     const std::vector<PhysicalTypePtr>& dataTypes,
                     const std::vector<Record::RecordFieldIdentifier>& fieldIdentifiers,
                     const std::vector<Record::RecordFieldIdentifier>& sortFieldIdentifiers)
    : operatorHandlerIndex(operatorHandlerIndex), dataTypes(dataTypes), fieldIdentifiers(fieldIdentifiers),
      sortFieldIdentifiers(sortFieldIdentifiers) {}

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

Value<> encodeData(Value<> value) {
    if (value->isType<Boolean>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<bool, bool>, value.as<Boolean>());
    } else if (value->isType<Int8>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<int8_t, int8_t>, value.as<Int8>());
    } else if (value->isType<Int16>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<int16_t, int16_t>, value.as<Int16>());
    } else if (value->isType<Int32>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<int32_t, int32_t>, value.as<Int32>());
    } else if (value->isType<UInt8>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<uint8_t, uint8_t>, value.as<UInt8>());
    } else if (value->isType<UInt16>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<uint16_t, uint16_t>, value.as<UInt16>());
    } else if (value->isType<UInt32>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<uint32_t, uint32_t>, value.as<UInt32>());
    } else if (value->isType<UInt64>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<uint64_t, uint64_t>, value.as<UInt64>());
    } else if (value->isType<Float>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<float, uint32_t>, value.as<Float>());
    } else if (value->isType<Double>()) {
        return Nautilus::FunctionCall("encodeData", encodeData<double, uint64_t>, value.as<Double>());
    } else {
        throw Exceptions::NotImplementedException("encodeData is not implemented for the given type.");
    }
}

void BatchSort::execute(ExecutionContext& ctx, Record& record) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto state = Nautilus::FunctionCall("getStackProxy", getStackProxy, globalOperatorHandler);

    auto entrySize = Nautilus::FunctionCall("getStateEntrySize", getStateEntrySize, globalOperatorHandler);
    auto vector = Interface::PagedVectorRef(state, entrySize->getValue());

    // create entry and store it in state
    auto entry = vector.allocateEntry();
    auto fields = record.getAllFields();
    // first store sort fields encoded for radix sort
    for (uint64_t i = 0; i < fields.size(); ++i) {
        if (fieldIdentifiers[i] == sortFieldIdentifiers[i]) {
            auto val = record.read(fields[i]);
            val = encodeData(val);
            entry.store(val);
            entry = entry + dataTypes[i]->size();
        }
    }
    // store all fields after the encoded sort fields
    for (uint64_t i = 0; i < fields.size(); ++i) {
        auto val = record.read(fields[i]);
        entry.store(val);
        entry = entry + dataTypes[i]->size();
    }
}
}// namespace NES::Runtime::Execution::Operators