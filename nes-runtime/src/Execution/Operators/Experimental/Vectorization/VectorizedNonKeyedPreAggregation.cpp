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

#include <Execution/Operators/Experimental/Vectorization/VectorizedNonKeyedPreAggregation.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/BlockDim.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/BlockIdx.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/FieldAccess.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/ThreadIdx.hpp>
#include <Nautilus/Tracing/TraceUtil.hpp>
#include <Util/Logger/Logger.hpp>

void addSliceToSliceStore(void* op, uint64_t workerId, uint64_t aggregate, uint64_t ts) {
    NES_INFO("(Worker {}) Adding {} to slice with timestamp {}", workerId, aggregate, ts);
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler*>(op);
    auto sliceStore = handler->getThreadLocalSliceStore(workerId);
    auto ptr = sliceStore->findSliceByTs(ts)->getState()->ptr;
    *reinterpret_cast<uint64_t*>(ptr) = aggregate;
};

namespace NES::Runtime::Execution::Operators {

VectorizedNonKeyedPreAggregation::VectorizedNonKeyedPreAggregation(
    std::shared_ptr<NonKeyedSlicePreAggregation> nonKeyedSlicePreAggregationOperator,
    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider,
    std::vector<Nautilus::Record::RecordFieldIdentifier> projections
)
    : nonKeyedSlicePreAggregationOperator(nonKeyedSlicePreAggregationOperator)
    , memoryProvider(std::move(memoryProvider))
    , projections(std::move(projections))
{

}

// TODO Move this method out of this source file to a more sensible place.
static Value<> getCompilerBuiltInVariable(const std::shared_ptr<BuiltInVariable>& builtInVariable) {
    auto ref = createNextValueReference(builtInVariable->getType());
    Tracing::TraceUtil::traceConstOperation(builtInVariable, ref);
    auto value = builtInVariable->getAsValue();
    value.ref = ref;
    return value;
}

void* getSliceStore() {
    return nullptr;
}

uint64_t sum(void* /*buffer*/, uint64_t /*tid*/, uint64_t /*offset*/) {
    return 0;
}

void VectorizedNonKeyedPreAggregation::setup(ExecutionContext& ctx) const {
    nonKeyedSlicePreAggregationOperator->setup(ctx);
}

void VectorizedNonKeyedPreAggregation::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    nonKeyedSlicePreAggregationOperator->open(ctx, rb);
}

void VectorizedNonKeyedPreAggregation::execute(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // Assume ingestion time for now. Thus, each record shares the same timestamp.
    // Assume sum aggregation for now. Thus, we can call reduce on the fields.

    auto blockDim = std::make_shared<BlockDim>();
    auto blockDim_x = getCompilerBuiltInVariable(blockDim->x());

    auto blockIdx = std::make_shared<BlockIdx>();
    auto blockIdx_x = getCompilerBuiltInVariable(blockIdx->x());

    auto threadIdx = std::make_shared<ThreadIdx>();
    auto threadIdx_x = getCompilerBuiltInVariable(threadIdx->x());

    auto threadId = blockIdx_x * blockDim_x + threadIdx_x;

    auto bufferAddress = recordBuffer.getBuffer();
    auto recordIndex = threadId.as<UInt64>();

    auto numberOfRecords = recordBuffer.getNumRecords();
    auto creationTs = recordBuffer.getCreatingTs();

    if (recordIndex < numberOfRecords) {
        auto aggregationFunctions = nonKeyedSlicePreAggregationOperator->getAggregationFunctions();
        for (const auto& aggregationFunction : aggregationFunctions) {
            auto fieldName = aggregationFunction->getInputFieldIdentifier();
            auto fieldIndex = memoryProvider->getMemoryLayoutPtr()->getFieldIndexFromName(fieldName);
            NES_ASSERT(fieldIndex, "Invalid field name");
            auto fieldOffset = memoryProvider->getMemoryLayoutPtr()->getFieldOffset(0, fieldIndex.value());
            auto fieldOffsetVal = Value<UInt64>(fieldOffset);
            auto aggregate = FunctionCall("sum", sum, bufferAddress, recordIndex, fieldOffsetVal);
            if (threadId == 0) {
                auto sliceStore = FunctionCall("getSliceStore", getSliceStore);
                sliceStore.store(aggregate);
                // TODO Timestamp for ingestion time is set in ctx in open() method which happens after kernel compilation.
                // auto timeFunction = nonKeyedSlicePreAggregationOperator->getTimeFunction();
                // auto record = memoryProvider->read(projections, bufferAddress, recordIndex);
                // auto timestampValue = timeFunction->getTs(ctx, record);
                auto sliceStoreTs = (sliceStore + aggregationFunction->getSize()).as<MemRef>();
                sliceStoreTs.store(creationTs);
            }
        }
    }

    if (hasChild()) {
        auto vectorizedChild = std::dynamic_pointer_cast<const VectorizableOperator>(child);
        vectorizedChild->execute(ctx, recordBuffer);
    }
}

void VectorizedNonKeyedPreAggregation::close(ExecutionContext& ctx, RecordBuffer& rb) const {
    nonKeyedSlicePreAggregationOperator->close(ctx, rb);
}

} // namespace NES::Runtime::Execution::Operators
