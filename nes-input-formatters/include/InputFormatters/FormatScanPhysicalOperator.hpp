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

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// @brief This basic scan operator extracts records from a base tuple buffer according to a memory layout.
/// Furthermore, it supports projection push down to eliminate unneeded reads
class FormatScanPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    FormatScanPhysicalOperator(
        std::vector<Record::RecordFieldIdentifier> projections,
        std::unique_ptr<InputFormatters::InputFormatterTaskPipeline> inputFormatterTaskPipeline,
        size_t configuredBufferSize);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::vector<Record::RecordFieldIdentifier> projections;
    std::shared_ptr<InputFormatters::InputFormatterTaskPipeline> taskPipeline;
    std::optional<PhysicalOperator> child;
    size_t configuredBufferSize;
};

}
