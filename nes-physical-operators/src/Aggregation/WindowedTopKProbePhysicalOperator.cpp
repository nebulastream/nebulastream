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

#include <Aggregation/WindowedTopKProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Aggregation/AggregationHashMapMerge.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/TopKAggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <Interface/RowHeap/RowHeapRef.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

WindowedTopKProbePhysicalOperator::WindowedTopKProbePhysicalOperator(
    ChainedHashMapConfig hashMapConfig,
    std::shared_ptr<TopKAggregationPhysicalFunction> topK,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , hashMapConfig(std::move(hashMapConfig))
    , topK(std::move(topK))
{
}

void WindowedTopKProbePhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    topK->setup(compilationContext);
    WindowProbePhysicalOperator::setup(executionCtx, compilationContext);
}

void WindowedTopKProbePhysicalOperator::probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto emittedWindow = static_cast<nautilus::val<EmittedAggregationWindow*>>(recordBuffer.getMemArea());
    const auto numberOfHashMaps = readValueFromMemRef<uint64_t>(getMemberRef(emittedWindow, &EmittedAggregationWindow::numberOfHashMaps));
    const auto windowInfoRef = getMemberRef(emittedWindow, &EmittedAggregationWindow::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    /// Merge the per-thread top rows of all slices of the window per key.
    const std::vector<std::shared_ptr<AggregationPhysicalFunction>> functions{topK};
    OwnedNautilusBuffer windowRows;
    createAggregationHashMap(executionCtx, hashMapConfig, windowRows);
    const BorrowedNautilusBuffer windowRowsRef = windowRows;
    for (nautilus::val<uint64_t> hashMapIdx = 0; hashMapIdx < numberOfHashMaps; ++hashMapIdx)
    {
        OwnedNautilusBuffer hashMap;
        nautilus::invoke(
            +[](TupleBuffer* parent, uint32_t hashMapIdx, TupleBuffer* hashMapBuffer)
            {
                INVARIANT(parent != nullptr, "Parent Tuplebuffer MUST NOT be null at this point");
                *hashMapBuffer = parent->loadChildBuffer(ChildBufferIndex{hashMapIdx});
            },
            recordBuffer.getReference(),
            hashMapIdx,
            hashMap.asArg());
        const BorrowedNautilusBuffer hashMapRef = hashMap;
        combineAggregationHashMap(executionCtx, hashMapConfig, functions, windowRowsRef, hashMapRef);
    }

    /// Emit the top rows of every key in their order.
    const ChainedHashMapRef windowMap{windowRowsRef, hashMapConfig};
    for (const auto entry : windowMap)
    {
        const ChainedHashMapRef::ChainedEntryRef entryRef{entry, windowRowsRef, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
        auto rowsBuffer
            = TopKAggregationPhysicalFunction::loadRows(static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea()), windowRowsRef);
        const RowHeapRef rows(BorrowedNautilusBuffer::from(rowsBuffer.asArg()), topK->getRowLayout());
        /// The window's rows are discarded after they are emitted, so they can be sorted in place.
        rows.sortRows(topK->getComparator());
        const auto numberOfRows = rows.size();
        for (nautilus::val<uint64_t> index = 0; index < numberOfRows; ++index)
        {
            auto row = rows.at(index);
            row.write(windowMetaData.startField.getFullyQualifiedName(), windowStart.convertToValue());
            row.write(windowMetaData.endField.getFullyQualifiedName(), windowEnd.convertToValue());
            executeChild(executionCtx, row);
        }
    }

    /// The merged rows hold everything the probe needs, so the emitted window and the child buffers it holds can go.
    nautilus::invoke(
        +[](EmittedAggregationWindow* emittedAggregationWindow) { emittedAggregationWindow->~EmittedAggregationWindow(); },
        emittedWindow);
}

}
