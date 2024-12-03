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
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical operator for all window sinks.
 * A window sink computes the final window result using the window slice store.
 */
class PhysicalWindowOperator : public PhysicalUnaryOperator
{
public:
    PhysicalWindowOperator(
        OperatorId id,
        std::shared_ptr<Schema> inputSchema,
        std::shared_ptr<Schema> outputSchema,
        std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition,
        std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler);
    const std::shared_ptr<Windowing::LogicalWindowDescriptor>& getWindowDefinition() const;
    std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> getOperatorHandler() const;
    std::unique_ptr<Runtime::Execution::Operators::TimeFunction> getTimeFunction() const;
    std::vector<std::unique_ptr<Runtime::Execution::Aggregation::AggregationFunction>>
    getAggregationFunctions(const Configurations::QueryCompilerConfiguration& options) const;
    std::pair<std::vector<Nautilus::Record::RecordFieldIdentifier>, std::vector<Nautilus::Record::RecordFieldIdentifier>>
    getKeyAndValueFields() const;
    ~PhysicalWindowOperator() override = default;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;

    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition;
    std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler;
};
}
