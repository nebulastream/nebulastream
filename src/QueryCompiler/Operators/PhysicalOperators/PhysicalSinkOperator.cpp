/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalSinkOperator::PhysicalSinkOperator(OperatorId id,
                                           OperatorId logicalOperatorId,
                                           SchemaPtr inputSchema,
                                           SchemaPtr outputSchema,
                                           SinkDescriptorPtr sinkDescriptor)
    : OperatorNode(id), logicalSourceOperatorId(logicalOperatorId), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      sinkDescriptor(std::move(sinkDescriptor)) {}

PhysicalOperatorPtr PhysicalSinkOperator::create(OperatorId logicalSourceOperatorId,
                                                 SchemaPtr inputSchema,
                                                 SchemaPtr outputSchema,
                                                 SinkDescriptorPtr sinkDescriptor) {
    return create(UtilityFunctions::getNextOperatorId(), logicalSourceOperatorId, std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(sinkDescriptor));
}

PhysicalOperatorPtr PhysicalSinkOperator::create(OperatorId id,
                                                 OperatorId logicalSourceOperatorId,
                                                 const SchemaPtr& inputSchema,
                                                 const SchemaPtr& outputSchema,
                                                 const SinkDescriptorPtr& sinkDescriptor) {
    return std::make_shared<PhysicalSinkOperator>(id, logicalSourceOperatorId, inputSchema, outputSchema, sinkDescriptor);
}

SinkDescriptorPtr PhysicalSinkOperator::getSinkDescriptor() { return sinkDescriptor; }

std::string PhysicalSinkOperator::toString() const { return "PhysicalSinkOperator"; }
OperatorId PhysicalSinkOperator::getLogicalOperatorId() { return logicalSourceOperatorId; }

OperatorNodePtr PhysicalSinkOperator::copy() {
    return create(id, logicalSourceOperatorId, inputSchema, outputSchema, sinkDescriptor);
}

}// namespace NES::QueryCompilation::PhysicalOperators
