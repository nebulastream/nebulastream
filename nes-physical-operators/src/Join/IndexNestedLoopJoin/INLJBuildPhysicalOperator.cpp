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

#include <Join/IndexNestedLoopJoin/INLJBuildPhysicalOperator.hpp>

#include <memory>
#include <string>
#include <utility>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <fmt/format.h>
#include <val_bool.hpp>

namespace NES
{

INLJBuildPhysicalOperator::INLJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<BTreeTupleLayout> tupleLayout,
    Record::RecordFieldIdentifier keyField,
    std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction), std::move(sliceStoreRef))
    , joinBuildSide(joinBuildSide)
    , tupleLayout(std::move(tupleLayout))
    , keyField(std::move(keyField))
{
}

void INLJBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    auto comparator = std::make_shared<BTreeComparator>(
        compilationContext,
        tupleLayout,
        fmt::format("inljBuildComparator:{}:{}", id.getRawValue(), static_cast<uint8_t>(joinBuildSide)),
        [keyField = keyField](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
        { return (lhs.read(keyField) < rhs.read(keyField)).getRawValueAs<nautilus::val<bool>>(); });
    activeComparator = comparator.get();
    comparators.emplace_back(std::move(comparator));
    WindowBuildPhysicalOperator::setup(executionCtx, compilationContext);
}

void INLJBuildPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    PRECONDITION(activeComparator != nullptr, "INLJ BTree comparator must be set up before execute");
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    const auto operatorHandler = localState->getOperatorHandler();
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto treeBuffer = sliceStoreRef->getDataStructureRef(
        timestamp, executionCtx.workerThreadId, operatorHandler, executionCtx.pipelineMemoryProvider.bufferProvider);
    const BTreeRef tree{BorrowedNautilusBuffer::from(treeBuffer.asArg()), tupleLayout};
    tree.append(record, executionCtx.pipelineMemoryProvider.bufferProvider, *activeComparator);
}

}
