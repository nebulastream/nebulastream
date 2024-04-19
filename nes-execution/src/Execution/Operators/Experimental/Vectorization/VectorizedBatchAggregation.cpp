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

#include <Execution/Operators/Experimental/Vectorization/Kernel.hpp>
#include <Execution/Operators/Experimental/Vectorization/VectorizedBatchAggregation.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/BlockDim.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/BlockIdx.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/FieldAccess.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/ThreadIdx.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Logger/Logger.hpp>
#include <deque>
#include <Execution/Aggregation/Experimental/Vectorization/VectorizedAggregationFunction.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>

namespace NES::Runtime::Execution::Operators {
VectorizedBatchAggregation::VectorizedBatchAggregation(std::shared_ptr<BatchAggregation> batchAggregation,
                                                       std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider)
    : batchAggregation(std::move(batchAggregation))
    , memoryProvider(std::move(memoryProvider))
{

}

void VectorizedBatchAggregation::execute(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    auto blockDim = std::make_shared<BlockDim>();
    auto blockDim_x = Kernel::getCompilerBuiltInVariable(blockDim->x());

    auto blockIdx = std::make_shared<BlockIdx>();
    auto blockIdx_x = Kernel::getCompilerBuiltInVariable(blockIdx->x());

    auto threadIdx = std::make_shared<ThreadIdx>();
    auto threadIdx_x = Kernel::getCompilerBuiltInVariable(threadIdx->x());

    auto threadId = blockIdx_x * blockDim_x + threadIdx_x;

    auto bufferAddress = recordBuffer.getBuffer();
    auto recordIndex = threadId.as<UInt64>();

    auto numberOfRecords = recordBuffer.getNumRecords();
    auto creationTs = recordBuffer.getCreatingTs();

    auto aggregationFunctions = batchAggregation->getAggregationFunctions();
    auto result = std::make_shared<Record>();
    //perfrom aggregation
    if (recordIndex < numberOfRecords) {
        for (size_t i = 0; i < aggregationFunctions.size(); i++) {
            auto const& aggregationFunction = aggregationFunctions[i];
            auto fieldName = aggregationFunction->getInputFieldIdentifier();
            auto fieldIndex = memoryProvider->getMemoryLayoutPtr()->getFieldIndexFromName(fieldName);
            NES_ASSERT(fieldIndex, "Invalid field name");
            auto fieldOffset = memoryProvider->getMemoryLayoutPtr()->getFieldOffset(0, fieldIndex.value());
            auto fieldOffsetVal = Value<UInt64>(fieldOffset);
            //TODO: for now only sum is implemented and assume input type uint64
//            aggregates[i] = aggregationFunction->callVectorizedFunction(bufferAddress, recordIndex, fieldOffsetVal);
            auto aggregate = FunctionCall("sum", Aggregation::VectorizedAggregationFunction::sum, bufferAddress, recordIndex, fieldOffsetVal);
            result->write(aggregationFunction->getResultFieldIdentifier(), aggregate);
            if (threadId == 0) {
                //TODO: Slice store is temporary
                auto sliceStore = FunctionCall("getSliceStore", Aggregation::VectorizedAggregationFunction::getSliceStore);
                sliceStore.store(aggregate);
                //TODO:might have to remove this
                auto sliceStoreTs = (sliceStore + aggregationFunction->getSize()).as<MemRef>();
                sliceStoreTs.store(creationTs);
            }
            (void) ctx; //silence unused parameter
        }
    }
    if (hasChild()) {
        //TODO: Pass result to child  in vectorized manner
        child->execute(ctx, *result);
    }
}

}