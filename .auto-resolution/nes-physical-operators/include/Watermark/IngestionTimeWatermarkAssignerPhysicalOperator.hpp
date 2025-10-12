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
#include <optional>
#include <Watermark/TimeFunction.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
/// @brief Watermark assignment operator.
/// Determines the watermark ts according to a WatermarkStrategyDescriptor and places it in the current buffer.
class IngestionTimeWatermarkAssignerPhysicalOperator : public PhysicalOperatorConcept
{
public:
    explicit IngestionTimeWatermarkAssignerPhysicalOperator(IngestionTimeFunction timeFunction);
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    IngestionTimeFunction timeFunction;
    std::optional<PhysicalOperator> child;
};

}
