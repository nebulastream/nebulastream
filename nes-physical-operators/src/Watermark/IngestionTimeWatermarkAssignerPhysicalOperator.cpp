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

#include <optional>
#include <utility>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Watermark/IngestionTimeWatermarkAssignerPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

IngestionTimeWatermarkAssignerPhysicalOperator::IngestionTimeWatermarkAssignerPhysicalOperator(IngestionTimeFunction timeFunction)
    : timeFunction(std::move(timeFunction)) { };

void IngestionTimeWatermarkAssignerPhysicalOperator::open(ExecutionContext& executionContext, CompilationContext& compilationContext, RecordBuffer& recordBuffer) const
{
    openChild(executionContext, compilationContext, recordBuffer);
    timeFunction.open(executionContext, compilationContext, recordBuffer);
    auto emptyRecord = Record();
    const auto tsField = [this](ExecutionContext& executionContext, CompilationContext& compilationContext)
    {
        auto emptyRecord = Record();
        return timeFunction.getTs(executionContext, compilationContext, emptyRecord);
    }(executionContext, compilationContext);
    if (const auto currentWatermark = executionContext.watermarkTs; tsField > currentWatermark)
    {
        executionContext.watermarkTs = tsField;
    }
}

void IngestionTimeWatermarkAssignerPhysicalOperator::execute(ExecutionContext& executionContext, CompilationContext& compilationContext, Record& record) const
{
    executeChild(executionContext, compilationContext, record);
}

std::optional<PhysicalOperator> IngestionTimeWatermarkAssignerPhysicalOperator::getChild() const
{
    return child;
}

void IngestionTimeWatermarkAssignerPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
