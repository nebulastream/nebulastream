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
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical watermark assignment operator.
 */
class PhysicalWatermarkAssignmentOperator : public PhysicalUnaryOperator
{
public:
    PhysicalWatermarkAssignmentOperator(
        OperatorId id,
        std::shared_ptr<Schema> inputSchema,
        std::shared_ptr<Schema> outputSchema,
        std::shared_ptr<Windowing::WatermarkStrategyDescriptor> watermarkStrategyDescriptor);
    static std::shared_ptr<PhysicalOperator> create(
        OperatorId id,
        const std::shared_ptr<Schema>& inputSchema,
        const std::shared_ptr<Schema>& outputSchema,
        const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor);
    static std::shared_ptr<PhysicalOperator> create(
        const std::shared_ptr<Schema>& inputSchema,
        const std::shared_ptr<Schema>& outputSchema,
        const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor);
    std::shared_ptr<Operator> copy() override;

    /**
    * @brief Returns the watermark strategy.
    * @return  std::shared_ptr<Windowing::WatermarkStrategyDescriptor>
    */
    std::shared_ptr<Windowing::WatermarkStrategyDescriptor> getWatermarkStrategyDescriptor() const;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

private:
    std::shared_ptr<Windowing::WatermarkStrategyDescriptor> watermarkStrategyDescriptor;
};
}
