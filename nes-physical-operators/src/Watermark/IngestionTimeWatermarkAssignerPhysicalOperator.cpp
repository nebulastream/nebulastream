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

#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Watermark/IngestionTimeWatermarkAssignerPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>

namespace NES
{

IngestionTimeWatermarkAssignerPhysicalOperator::IngestionTimeWatermarkAssignerPhysicalOperator(IngestionTimeFunction timeFunction)
    : timeFunction(std::move(timeFunction)) {};

void IngestionTimeWatermarkAssignerPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PhysicalOperatorConcept::open(executionCtx, recordBuffer);
    timeFunction.open(executionCtx, recordBuffer);
    auto emptyRecord = Record();
    const auto tsField = timeFunction.getTs(executionCtx, emptyRecord);
    const auto currentWatermark = executionCtx.watermarkTs;
    if (tsField > currentWatermark)
    {
        executionCtx.watermarkTs = tsField;
    }
}

void IngestionTimeWatermarkAssignerPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    PhysicalOperatorConcept::execute(executionCtx, record);
}

}
