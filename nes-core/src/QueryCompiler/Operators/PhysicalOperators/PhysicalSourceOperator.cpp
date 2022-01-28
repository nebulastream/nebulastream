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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalSourceOperator::PhysicalSourceOperator(OperatorId id,
                                               SchemaPtr inputSchema,
                                               SchemaPtr outputSchema,
                                               SourceDescriptorPtr sourceDescriptor)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      sourceDescriptor(std::move(sourceDescriptor)) {}

PhysicalOperatorPtr PhysicalSourceOperator::create(OperatorId id,
                                                   const SchemaPtr& inputSchema,
                                                   const SchemaPtr& outputSchema,
                                                   const SourceDescriptorPtr& sourceDescriptor) {
    return std::make_shared<PhysicalSourceOperator>(id, inputSchema, outputSchema, sourceDescriptor);
}

PhysicalOperatorPtr
PhysicalSourceOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, SourceDescriptorPtr sourceDescriptor) {
    return create(Util::getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(sourceDescriptor));
}

SourceDescriptorPtr PhysicalSourceOperator::getSourceDescriptor() { return sourceDescriptor; }

std::string PhysicalSourceOperator::toString() const { return "PhysicalSourceOperator"; }

OperatorNodePtr PhysicalSourceOperator::copy() { return create(id, inputSchema, outputSchema, sourceDescriptor); }

}// namespace NES::QueryCompilation::PhysicalOperators