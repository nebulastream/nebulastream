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
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
namespace NES::QueryCompilation::PhysicalOperators
{

class PhysicalAggregationBuild final : public PhysicalWindowOperator
{
public:
    static PhysicalOperatorPtr create(
        OperatorId id,
        SchemaPtr inputSchema,
        SchemaPtr outputSchema,
        Windowing::LogicalWindowDescriptorPtr windowDefinition,
        std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler);

    std::shared_ptr<Operator> copy() override;

private:
    PhysicalAggregationBuild(
        OperatorId id,
        SchemaPtr inputSchema,
        SchemaPtr outputSchema,
        Windowing::LogicalWindowDescriptorPtr windowDefinition,
        std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler);
};

}
