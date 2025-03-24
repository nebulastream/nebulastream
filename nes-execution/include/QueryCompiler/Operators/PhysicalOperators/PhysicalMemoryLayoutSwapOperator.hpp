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
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/// @brief PhysicalSwapOperator is a physical operator that swaps the layout of the schema.
/// This operator gets transformed into a scan operator and an emit operator in the lower physical to nautilus phase.
class PhysicalMemoryLayoutSwapOperator final : public PhysicalUnaryOperator, public AbstractScanOperator, public AbstractEmitOperator
{
public:
    PhysicalMemoryLayoutSwapOperator(
        OperatorId id, const std::shared_ptr<Schema>& inputSchema, const std::shared_ptr<Schema>& outputSchema);
    static std::shared_ptr<PhysicalOperator>
    create(OperatorId id, const std::shared_ptr<Schema>& inputSchema, const std::shared_ptr<Schema>& outputSchema);
    static std::shared_ptr<PhysicalOperator>
    create(const std::shared_ptr<Schema>& inputSchema, const std::shared_ptr<Schema>& outputSchema);
    std::shared_ptr<Operator> copy() override;

protected:
    std::string toString() const override;
};
}
