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

#include <Operators/LogicalOperators/UnaryLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>

namespace NES
{

/// @brief Watermark assignment operator, creates a watermark timestamp per input buffer.
class WatermarkAssignerLogicalOperator : public UnaryLogicalOperator
{
public:
    WatermarkAssignerLogicalOperator(
        const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor, OperatorId id);

    /// @brief Returns the watermark strategy.
    /// @return std::shared_ptr<Windowing::WatermarkStrategyDescriptor>
    std::shared_ptr<Windowing::WatermarkStrategyDescriptor> getWatermarkStrategyDescriptor() const;

    [[nodiscard]] bool operator==(const Operator& rhs) const override;

    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;

    std::shared_ptr<Operator> clone() const override;
    bool inferSchema() override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

private:
    std::shared_ptr<Windowing::WatermarkStrategyDescriptor> watermarkStrategyDescriptor;
};


}
