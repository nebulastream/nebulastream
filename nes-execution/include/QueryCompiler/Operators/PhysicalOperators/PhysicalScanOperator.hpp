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
#include <ostream>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical Scan operator.
 */
class PhysicalScanOperator : public PhysicalUnaryOperator, public AbstractScanOperator
{
public:
    /**
     * @brief Constructor for the physical scan operator
     * @param id operator id
     * @param outputSchema output schema
     */
    PhysicalScanOperator(OperatorId id, Schema outputSchema);

    /**
     * @brief Creates for the physical scan operator
     * @param id operator id
     * @param outputSchema output schema
     */
    static std::shared_ptr<PhysicalOperator> create(OperatorId id, Schema outputSchema);

    /**
     * @brief Constructor for the physical scan operator
     * @param outputSchema output schema
     */
    static std::shared_ptr<PhysicalOperator> create(Schema outputSchema);
    std::shared_ptr<Operator> copy() override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;
};
}
