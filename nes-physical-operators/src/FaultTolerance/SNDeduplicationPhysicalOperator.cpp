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
#include <Util/StdInt.hpp>


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

bool filterAndBufferProxy(OperatorHandler* handler, TupleBuffer* buffer)
{
    auto* deduplicationHandler = dynamic_cast<SNDeduplicationOperatorHandler*>(handler);
    return deduplicationHandler->filterAndBuffer(buffer);
}

TupleBuffer* probeProxy(OperatorHandler* handler, TupleBuffer* buffer)
{
    auto* deduplicationHandler = dynamic_cast<SNDeduplicationOperatorHandler*>(handler);
    return deduplicationHandler->probe(buffer);
}

void finalizeSNProxy(OperatorHandler* handler, TupleBuffer* buffer)
{
    auto* deduplicationHandler = dynamic_cast<SNDeduplicationOperatorHandler*>(handler);
    deduplicationHandler->finalizeSN(buffer);
}
void SNDeduplicationPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{

    //ScanPhysicalOperator::open(executionCtx, recordBuffer);
    //ScanPhysicalOperator::close(executionCtx, recordBuffer);
    //auto localState = std::make_unique<SND::DeduplicationLocalState>();
    auto globalHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    auto probeFlag = nautilus::invoke(filterAndBufferProxy, globalHandler, recordBuffer.getReference());
    if (probeFlag)
    {
        for (auto buffer = nautilus::invoke(probeProxy, globalHandler, recordBuffer.getReference()); buffer != nautilus::val<TupleBuffer*>(nullptr);
             buffer = nautilus::invoke(probeProxy, globalHandler, recordBuffer.getReference()))
        {
            RecordBuffer recordBufferToEmit(buffer);
            ScanPhysicalOperator::open(executionCtx, recordBufferToEmit);
            ScanPhysicalOperator::close(executionCtx, recordBufferToEmit);
            executionCtx.resetLocalPipelineState();
        }
        nautilus::invoke(finalizeSNProxy, globalHandler, recordBuffer.getReference());
    }
}

void SNDeduplicationPhysicalOperator::execute(ExecutionContext&, Record&) const
{
    /// NOOP
}

void SNDeduplicationPhysicalOperator::close(ExecutionContext&, RecordBuffer&) const
{
    /// NOOP
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