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

#include <Operators/Operator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief General abstraction for all physical operators.
 * A physical operator represents the concrete realization of a logical operator.
 * It can be a direct mapping, e.g., a LogicalSelectionOperator maps to a PhysicalSelectionOperator.
 * Other logical operators can also result in multiple physical operators,
 * e.g., CentralWindowOperator results in a SlicePreAggregation and a WindowSinkOperator.
 * This mapping is called lowering and is defined in a PhysicalOperatorProvider.
 */
class PhysicalOperator : public virtual Operator
{
protected:
    explicit PhysicalOperator(OperatorId id);

    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;

public:
    ~PhysicalOperator() noexcept override = default;
};
}
