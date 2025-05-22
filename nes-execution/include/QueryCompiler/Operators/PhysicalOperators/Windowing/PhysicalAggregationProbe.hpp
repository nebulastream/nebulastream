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
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

class PhysicalAggregationProbe final : public PhysicalWindowOperator
{
public:
    WindowMetaData getWindowMetaData() const;
    static std::shared_ptr<PhysicalOperator> create(
        OperatorId id,
        std::shared_ptr<Schema> inputSchema,
        std::shared_ptr<Schema> outputSchema,
        std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition,
        std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler,
        WindowMetaData windowMetaData);

    std::shared_ptr<Operator> copy() override;

private:
    PhysicalAggregationProbe(
        OperatorId id,
        std::shared_ptr<Schema> inputSchema,
        std::shared_ptr<Schema> outputSchema,
        std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition,
        std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler,
        WindowMetaData windowMetaData);
    WindowMetaData windowMetaData;
};

}
