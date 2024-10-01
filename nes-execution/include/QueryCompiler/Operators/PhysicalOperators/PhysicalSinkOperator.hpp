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

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical Sink operator.
 */
class PhysicalSinkOperator : public PhysicalUnaryOperator, public AbstractEmitOperator, public AbstractScanOperator
{
public:
    PhysicalSinkOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, SinkDescriptorPtr sinkDescriptor);
    static PhysicalOperatorPtr
    create(OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, const SinkDescriptorPtr& sinkDescriptor);
    static PhysicalOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema, SinkDescriptorPtr sinkDescriptor);
    SinkDescriptorPtr getSinkDescriptor();

    std::string toString() const override;
    OperatorPtr copy() override;

private:
    SinkDescriptorPtr sinkDescriptor;
};
} /// namespace NES::QueryCompilation::PhysicalOperators
