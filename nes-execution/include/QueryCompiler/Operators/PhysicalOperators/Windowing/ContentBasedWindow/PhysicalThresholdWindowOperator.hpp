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
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{
/**
 * @brief Physical operator ThresholdWindow.
 * This class represent physical operator for ThresholdWindow. It stores the operator handler which later can be used in the
 * executable operator.
 */
class PhysicalThresholdWindowOperator : public PhysicalUnaryOperator
{
public:
    /**
     * @brief constructor of the physical operator of threshold window
     * @param id of the operator
     * @param inputSchema input schema for the operator
     * @param outputSchema output schema for the operator
     * @param operatorHandler pointer to the operator handler of the threshold window (of type ThresholdWindowOperatorHandler)
     */
    PhysicalThresholdWindowOperator(
        OperatorId id,
        std::shared_ptr<Schema> inputSchema,
        std::shared_ptr<Schema> outputSchema,
        std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition);

    static std::shared_ptr<PhysicalThresholdWindowOperator> create(
        const std::shared_ptr<Schema>& inputSchema,
        const std::shared_ptr<Schema>& outputSchema,
        const std::shared_ptr<Windowing::LogicalWindowDescriptor>& windowDefinition);

    std::shared_ptr<Windowing::LogicalWindowDescriptor> getWindowDefinition();

    std::shared_ptr<Operator> copy() override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

private:
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition;
};

}
