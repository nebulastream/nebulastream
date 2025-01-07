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

#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical operator for all window sinks.
 * A window sink computes the final window result using the window slice store.
 */
class PhysicalWindowSinkOperator : public PhysicalWindowOperator, public AbstractScanOperator
{
public:
    PhysicalWindowSinkOperator(
        OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::LogicalWindowDescriptorPtr windowDefinition);
    static PhysicalOperatorPtr create(
        OperatorId id,
        const SchemaPtr& inputSchema,
        const SchemaPtr& outputSchema,
        const Windowing::LogicalWindowDescriptorPtr& windowDefinition);
    std::shared_ptr<Operator> copy() override;

protected:
    std::string toString() const override;
};
}
