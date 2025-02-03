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
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{
class PhysicalWindowTrigger final : public PhysicalUnaryOperator, public AbstractScanOperator, public AbstractEmitOperator
{
public:
    static std::shared_ptr<PhysicalOperator> create(
        OperatorId id,
        std::shared_ptr<Schema> inputSchema,
        std::shared_ptr<Schema> outputSchema,
        std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler)
    {
        return std::make_shared<PhysicalWindowTrigger>(
            PhysicalWindowTrigger(std::move(id), std::move(inputSchema), std::move(outputSchema), std::move(windowHandler)));
    }
    std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> getOperatorHandler() const { return windowHandler; }

    std::shared_ptr<Operator> copy() override { return create(id, inputSchema, outputSchema, windowHandler); }

protected:
    std::string toString() const override { return fmt::format("PhysicalWindowTrigger: {}", id); }

private:
    explicit PhysicalWindowTrigger(
        OperatorId id,
        std::shared_ptr<Schema> inputSchema,
        std::shared_ptr<Schema> outputSchema,
        std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler)
        : Operator(id)
        , PhysicalUnaryOperator(std::move(id), std::move(inputSchema), std::move(outputSchema))
        , windowHandler(std::move(windowHandler))
    {
    }
    std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler;
};
}
