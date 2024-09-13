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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

class PhysicalSourceOperator : public PhysicalUnaryOperator, public AbstractScanOperator
{
public:
    PhysicalSourceOperator(
        OperatorId id,
        OriginId originId,
        SchemaPtr inputSchema,
        SchemaPtr outputSchema,
        std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor);

    static std::shared_ptr<PhysicalSourceOperator> create(
        OperatorId id,
        OriginId originId,
        const SchemaPtr& inputSchema,
        const SchemaPtr& outputSchema,
        std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor);

    static std::shared_ptr<PhysicalSourceOperator>
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::unique_ptr<Sources::SourceDescriptor>&& sourceDescriptor);

    const SourceDescriptor& getSourceDescriptorRef() const;

    void setOriginId(OriginId originId);

    OriginId getOriginId();
    std::string toString() const override;
    OperatorPtr copy() override;

private:
    const std::unique_ptr<Sources::SourceDescriptor> sourceDescriptor;
    OriginId originId;
};
} /// namespace NES::QueryCompilation::PhysicalOperators
