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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{
/**
 * @brief The physical multiplex operator
 * This operator has multiple upstream operators and forwards it to exactly one down-stream operator.
 * Thus it has multiple child nodes and one parent nodes.
 * Example query plan:
 *
 * SourceThread --- OperatorX ---
 *                             \
 *                              --- Multiplex --- OperatorZ --- DataSink
 *                              /
 * SourceThread --- OperatorY ---
 *
 */
class PhysicalUnionOperator : public PhysicalBinaryOperator
{
public:
    PhysicalUnionOperator(
        OperatorId id,
        const std::shared_ptr<Schema>& leftSchema,
        const std::shared_ptr<Schema>& rightSchema,
        const std::shared_ptr<Schema>& outputSchema);
    static std::shared_ptr<PhysicalOperator> create(
        OperatorId id,
        const std::shared_ptr<Schema>& leftSchema,
        const std::shared_ptr<Schema>& rightSchema,
        const std::shared_ptr<Schema>& outputSchema);
    static std::shared_ptr<PhysicalOperator> create(OperatorId id, const std::shared_ptr<Schema>& schema);
    static std::shared_ptr<PhysicalOperator> create(const std::shared_ptr<Schema>& schema);
    std::shared_ptr<Operator> copy() override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;
};

}
