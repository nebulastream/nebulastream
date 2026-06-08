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

#include <memory>
#include <utility>
#include <FaultTolerance/SNDeduplicationOperatorHandler.hpp>
#include <FaultTolerance/SNDeduplicationPhysicalOperator.hpp>

namespace NES
{


void startHandlerProxy(OperatorHandler* handler)
{
    auto* deduplicationHandler = dynamic_cast<SNDeduplicationOperatorHandler*>(handler);
    deduplicationHandler->init();
}

void stopHandlerProxy(OperatorHandler* handler)
{
    auto* deduplicationHandler = dynamic_cast<SNDeduplicationOperatorHandler*>(handler);
    deduplicationHandler->save();
}


void SNDeduplicationPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    (void)compilationContext;
    invoke(startHandlerProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerId));
}

void SNDeduplicationPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    invoke(stopHandlerProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerId));
}

bool checkDuplicate(OperatorHandler* handler, OriginId oid, SequenceNumber sn)
{
    auto* deduplicationHandler = dynamic_cast<SNDeduplicationOperatorHandler*>(handler);
    return deduplicationHandler->checkAndInsert(oid, sn);
}

void SNDeduplicationPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    auto localState = std::make_unique<SND::DeduplicationLocalState>();
    auto globalHandler = ctx.getGlobalOperatorHandler(operatorHandlerId);

    auto originID = recordBuffer.getOriginId();
    auto seqNumber = recordBuffer.getSequenceNumber();
    localState->skipBuffer = nautilus::invoke(checkDuplicate, globalHandler, originID, seqNumber);
    if (!localState->skipBuffer)
    {
        openChild(ctx, recordBuffer);
    }

    ctx.setLocalOperatorState(id, std::move(localState));
}

void SNDeduplicationPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* const localState = dynamic_cast<SND::DeduplicationLocalState*>(ctx.getLocalState(id));
    if (!localState->skipBuffer)
    {
        executeChild(ctx, record);
    }
}

void SNDeduplicationPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    auto* const localState = dynamic_cast<SND::DeduplicationLocalState*>(ctx.getLocalState(id));

    if (!localState->skipBuffer)
    {
        closeChild(ctx, recordBuffer);
    }
}

[[nodiscard]] std::optional<PhysicalOperator> SNDeduplicationPhysicalOperator::getChild() const
{
    return child;
}

void SNDeduplicationPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}