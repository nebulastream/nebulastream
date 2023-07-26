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

#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinBuildOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalHashJoinBuildOperator::PhysicalHashJoinBuildOperator(
    const OperatorId id,
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler,
    const JoinBuildSideType buildSide,
    const std::string& timeStampFieldName,
    const std::string& joinFieldName)
    : OperatorNode(id), PhysicalHashJoinOperator(operatorHandler), PhysicalUnaryOperator(id, inputSchema, outputSchema),
      timeStampFieldName(timeStampFieldName), joinFieldName(joinFieldName), buildSide(buildSide) {}

PhysicalOperatorPtr
PhysicalHashJoinBuildOperator::create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler,
                                      const JoinBuildSideType buildSide,
                                      const std::string& timeStampFieldName,
                                      const std::string& joinFieldName) {

    return std::make_shared<PhysicalHashJoinBuildOperator>(id,
                                                           inputSchema,
                                                           outputSchema,
                                                           operatorHandler,
                                                           buildSide,
                                                           timeStampFieldName,
                                                           joinFieldName);
}

PhysicalOperatorPtr
PhysicalHashJoinBuildOperator::create(const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler,
                                      const JoinBuildSideType buildSide,
                                      const std::string& timeStampFieldName,
                                      const std::string& joinFieldName) {
    return create(Util::getNextOperatorId(),
                  inputSchema,
                  outputSchema,
                  operatorHandler,
                  buildSide,
                  timeStampFieldName,
                  joinFieldName);
}

std::string PhysicalHashJoinBuildOperator::toString() const { return "PhysicalHashJoinBuildOperator"; }

OperatorNodePtr PhysicalHashJoinBuildOperator::copy() {
    auto result = create(id, inputSchema, outputSchema, operatorHandler, buildSide, timeStampFieldName, joinFieldName);
    result->addAllProperties(properties);
    return result;
}

JoinBuildSideType PhysicalHashJoinBuildOperator::getBuildSide() const { return buildSide; }

const std::string& PhysicalHashJoinBuildOperator::getTimeStampFieldName() const { return timeStampFieldName; }

const std::string& PhysicalHashJoinBuildOperator::getJoinFieldName() const { return joinFieldName; }
}// namespace NES::QueryCompilation::PhysicalOperators