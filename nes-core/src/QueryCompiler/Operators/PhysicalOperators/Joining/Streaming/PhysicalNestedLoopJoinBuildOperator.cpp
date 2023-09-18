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

#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinBuildOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalNestedLoopJoinBuildOperator::PhysicalNestedLoopJoinBuildOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Runtime::Execution::Operators::NLJOperatorHandlerPtr operatorHandler,
    JoinBuildSideType buildSide,
    std::string timeStampFieldName,
    std::string joinFieldName)
    : OperatorNode(id), PhysicalNestedLoopJoinOperator(std::move(operatorHandler)),
      PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      timeStampFieldName(std::move(timeStampFieldName)), joinFieldName(std::move(joinFieldName)), buildSide(buildSide) {}

PhysicalOperatorPtr
PhysicalNestedLoopJoinBuildOperator::create(OperatorId id,
                                            const SchemaPtr& inputSchema,
                                            const SchemaPtr& outputSchema,
                                            const Runtime::Execution::Operators::NLJOperatorHandlerPtr& operatorHandler,
                                            JoinBuildSideType buildSide,
                                            const std::string& timeStampFieldName,
                                            const std::string& joinFieldName) {

    return std::make_shared<PhysicalNestedLoopJoinBuildOperator>(id,
                                                                 inputSchema,
                                                                 outputSchema,
                                                                 operatorHandler,
                                                                 buildSide,
                                                                 timeStampFieldName,
                                                                 joinFieldName);
}

PhysicalOperatorPtr
PhysicalNestedLoopJoinBuildOperator::create(const SchemaPtr& inputSchema,
                                            const SchemaPtr& outputSchema,
                                            const Runtime::Execution::Operators::NLJOperatorHandlerPtr& operatorHandler,
                                            JoinBuildSideType buildSide,
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

std::string PhysicalNestedLoopJoinBuildOperator::toString() const { return "PhysicalNestedLoopJoinBuildOperator"; }

OperatorNodePtr PhysicalNestedLoopJoinBuildOperator::copy() {
    auto result = create(id, inputSchema, outputSchema, operatorHandler, buildSide, timeStampFieldName, joinFieldName);
    result->addAllProperties(properties);
    return result;
}

JoinBuildSideType PhysicalNestedLoopJoinBuildOperator::getBuildSide() const { return buildSide; }

const std::string& PhysicalNestedLoopJoinBuildOperator::getTimeStampFieldName() const { return timeStampFieldName; }

const std::string& PhysicalNestedLoopJoinBuildOperator::getJoinFieldName() const { return joinFieldName; }
}// namespace NES::QueryCompilation::PhysicalOperators