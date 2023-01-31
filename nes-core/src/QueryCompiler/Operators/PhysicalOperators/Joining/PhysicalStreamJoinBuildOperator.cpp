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

#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinBuildOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalStreamJoinBuildOperator::PhysicalStreamJoinBuildOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr operatorHandler,
    JoinBuildSideType buildSide,
    std::string timeStampFieldName)
    : OperatorNode(id), PhysicalStreamJoinOperator(std::move(operatorHandler), id),
      PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      timeStampFieldName(std::move(timeStampFieldName)), buildSide(buildSide) {}

PhysicalOperatorPtr
PhysicalStreamJoinBuildOperator::create(OperatorId id,
                                        const SchemaPtr& inputSchema,
                                        const SchemaPtr& outputSchema,
                                        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                        JoinBuildSideType buildSide,
                                        const std::string& timeStampFieldName) {

    return std::make_shared<PhysicalStreamJoinBuildOperator>(id,
                                                             inputSchema,
                                                             outputSchema,
                                                             operatorHandler,
                                                             buildSide,
                                                             timeStampFieldName);
}

PhysicalOperatorPtr
PhysicalStreamJoinBuildOperator::create(const SchemaPtr& inputSchema,
                                        const SchemaPtr& outputSchema,
                                        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                        JoinBuildSideType buildSide,
                                        const std::string& timeStampFieldName) {
    return create(Util::getNextOperatorId(), inputSchema, outputSchema, operatorHandler, buildSide, timeStampFieldName);
}

std::string PhysicalStreamJoinBuildOperator::toString() const { return "PhysicalStreamJoinBuildOperator"; }

OperatorNodePtr PhysicalStreamJoinBuildOperator::copy() {
    return create(id, inputSchema, outputSchema, operatorHandler, buildSide, timeStampFieldName);
}

JoinBuildSideType PhysicalStreamJoinBuildOperator::getBuildSide() const { return buildSide; }

const std::string& PhysicalStreamJoinBuildOperator::getTimeStampFieldName() const { return timeStampFieldName; }
}// namespace NES::QueryCompilation::PhysicalOperators