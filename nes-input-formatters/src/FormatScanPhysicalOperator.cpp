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


#include <InputFormatters/FormatScanPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/StdInt.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

#include "InputFormatterTask.hpp"
#include "NativeInputFormatIndexer.hpp"
#include "RawValueParser.hpp"

namespace NES
{


FormatScanPhysicalOperator::FormatScanPhysicalOperator(
    std::vector<Record::RecordFieldIdentifier> projections,
    std::unique_ptr<InputFormatters::InputFormatterTaskPipeline> inputFormatterTaskPipeline,
    const size_t configuredBufferSize,
    const bool isFirstOperatorAfterSource)
    : projections(std::move(projections))
    , taskPipeline(std::move(inputFormatterTaskPipeline))
    , configuredBufferSize(configuredBufferSize)
    , isFirstOperatorAfterSource(isFirstOperatorAfterSource)
{
}

void FormatScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    this->taskPipeline->scan(executionCtx, recordBuffer, child.value(), projections, configuredBufferSize, isFirstOperatorAfterSource);
}

std::optional<PhysicalOperator> FormatScanPhysicalOperator::getChild() const
{
    return child;
}

void FormatScanPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
