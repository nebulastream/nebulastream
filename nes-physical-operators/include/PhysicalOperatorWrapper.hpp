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

#include <DataTypes/Schema.hpp>
#include <PhysicalOperator.hpp>

#pragma once

namespace NES
{
/// The wrapper provides all information of a physical operator needed for correct pipeline construction during query compilation.
/// The wrapper is removed after pipeline creation. Thus, our physical operators only contain information needed for the actual execution.
class PhysicalOperatorWrapper
{
public:
    enum class PipelineLocation : uint8_t
    {
        SCAN, /// pipeline scan
        EMIT, /// pipeline emit
        INTERMEDIATE, /// neither of them, intermediate operator
    };

    PhysicalOperatorWrapper(PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema);
    PhysicalOperatorWrapper(PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema, PipelineLocation pipelineLocation);
    PhysicalOperatorWrapper(
        PhysicalOperator physicalOperator,
        Schema inputSchema,
        Schema outputSchema,
        std::optional<OperatorHandlerId> handlerId,
        std::optional<std::shared_ptr<OperatorHandler>> handler,
        PipelineLocation pipelineLocation);
    PhysicalOperatorWrapper(
        PhysicalOperator physicalOperator,
        Schema inputSchema,
        Schema outputSchema,
        std::optional<OperatorHandlerId> handlerId,
        std::optional<std::shared_ptr<OperatorHandler>> handler,
        PipelineLocation pipelineLocation,
        std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children);

    /// for compatibility with free functions requiring getChildren()
    [[nodiscard]] std::vector<std::shared_ptr<PhysicalOperatorWrapper>> getChildren() const;

    /// Returns a string representation of the wrapper
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] const PhysicalOperator& getPhysicalOperator() const;
    [[nodiscard]] const std::optional<Schema>& getInputSchema() const;
    [[nodiscard]] const std::optional<Schema>& getOutputSchema() const;

    void addChild(const std::shared_ptr<PhysicalOperatorWrapper>& child);
    void setChildren(const std::vector<std::shared_ptr<PhysicalOperatorWrapper>>& newChildren);

    [[nodiscard]] const std::optional<std::shared_ptr<OperatorHandler>>& getHandler() const;
    [[nodiscard]] const std::optional<OperatorHandlerId>& getHandlerId() const;
    [[nodiscard]] PipelineLocation getPipelineLocation() const;

private:
    PhysicalOperator physicalOperator;
    std::optional<Schema> inputSchema;
    std::optional<Schema> outputSchema;
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children;

    std::optional<std::shared_ptr<OperatorHandler>> handler;
    std::optional<OperatorHandlerId> handlerId;
    PipelineLocation pipelineLocation;
};
}
