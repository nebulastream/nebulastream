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

#include <Sample/SampleTriggerPhysicalOperator.hpp>

#include <memory>
#include <utility>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

SampleTriggerPhysicalOperator::SampleTriggerPhysicalOperator(
    const OperatorHandlerId operatorHandlerId, std::unique_ptr<TimeFunction> timeFunction, std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction), std::move(sliceStoreRef))
{
}

void SampleTriggerPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    const auto timestamp = timeFunction->getTs(ctx, record);
    ctx.watermarkTs = timestamp;
    sliceStoreRef->getDataStructureRef(
        timestamp, ctx.workerThreadId, localState->getOperatorHandler(), ctx.pipelineMemoryProvider.bufferProvider);
}

}
