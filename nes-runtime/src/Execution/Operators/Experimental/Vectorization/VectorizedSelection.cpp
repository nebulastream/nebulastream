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

#include <Execution/Operators/Experimental/Vectorization/VectorizedSelection.hpp>

#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/BlockDim.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/BlockIdx.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/FieldAccess.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/ThreadIdx.hpp>
#include <Nautilus/Tracing/TraceUtil.hpp>

namespace NES::Runtime::Execution::Operators {

VectorizedSelection::VectorizedSelection(const Expressions::ExpressionPtr& expression,
    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider,
    std::vector<Nautilus::Record::RecordFieldIdentifier> projections)
    : expression(expression)
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

void setAsValidInMetadata(uint64_t /*recordIndex*/) {}

void VectorizedSelection::execute(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
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

    if (recordIndex < numberOfRecords) {
        auto record = memoryProvider->read(projections, bufferAddress, recordIndex);
        if (expression->execute(record)) {
            FunctionCall("setAsValidInMetadata", setAsValidInMetadata, recordIndex);
            memoryProvider->write(recordIndex, bufferAddress, record);
        }
    }

    if (hasChild()) {
        auto vectorizedChild = std::dynamic_pointer_cast<const VectorizableOperator>(child);
        vectorizedChild->execute(ctx, recordBuffer);
    }
}

} // namespace NES::Runtime::Execution::Operators
