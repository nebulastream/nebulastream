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

#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinBuildOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalStreamJoinBuildOperator::create(OperatorId id,
                                                            const SchemaPtr& inputSchema,
                                                            const SchemaPtr& outputSchema,
                                                            const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                                            const JoinBuildSideType buildSide,
                                                            const std::string& timeStampFieldName,
                                                            const std::string& joinFieldName,
                                                            QueryCompilation::StreamJoinStrategy joinStrategy,
                                                            QueryCompilation::WindowingStrategy windowingStrategy) {
    return std::make_shared<PhysicalStreamJoinBuildOperator>(id,
                                                           inputSchema,
                                                           outputSchema,
                                                           operatorHandler,
                                                           buildSide,
                                                           timeStampFieldName,
                                                           joinFieldName,
                                                           joinStrategy,
                                                           windowingStrategy);
}
PhysicalOperatorPtr PhysicalStreamJoinBuildOperator::create(const SchemaPtr& inputSchema,
                                                            const SchemaPtr& outputSchema,
                                                            const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                                            const JoinBuildSideType buildSide,
                                                            const std::string& timeStampFieldName,
                                                            const std::string& joinFieldName,
                                                            QueryCompilation::StreamJoinStrategy joinStrategy,
                                                            QueryCompilation::WindowingStrategy windowingStrategy) {
    return create(Util::getNextOperatorId(),
                  inputSchema,
                  outputSchema,
                  operatorHandler,
                  buildSide,
                  timeStampFieldName,
                  joinFieldName,
                  joinStrategy,
                  windowingStrategy);
}

PhysicalStreamJoinBuildOperator::PhysicalStreamJoinBuildOperator(const OperatorId id,
                                                                 const SchemaPtr& inputSchema,
                                                                 const SchemaPtr& outputSchema,
                                                                 const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                                                 const JoinBuildSideType buildSide,
                                                                 const std::string& timeStampFieldName,
                                                                 const std::string& joinFieldName,
                                                                 QueryCompilation::StreamJoinStrategy joinStrategy,
                                                                 QueryCompilation::WindowingStrategy windowingStrategy)
    : OperatorNode(id), PhysicalStreamJoinOperator(operatorHandler, joinStrategy, windowingStrategy),
      PhysicalUnaryOperator(id, inputSchema, outputSchema),
      timeStampFieldName(timeStampFieldName), joinFieldName(joinFieldName), buildSide(buildSide) {}


std::string PhysicalStreamJoinBuildOperator::toString() const { return "PhysicalStreamJoinBuildOperator"; }

OperatorNodePtr PhysicalStreamJoinBuildOperator::copy() {
    auto result = create(id, inputSchema, outputSchema, joinOperatorHandler, buildSide, timeStampFieldName, joinFieldName,
                         getJoinStrategy(), getWindowingStrategy());
    result->addAllProperties(properties);
    return result;
}

JoinBuildSideType PhysicalStreamJoinBuildOperator::getBuildSide() const { return buildSide; }

const std::string& PhysicalStreamJoinBuildOperator::getTimeStampFieldName() const { return timeStampFieldName; }

const std::string& PhysicalStreamJoinBuildOperator::getJoinFieldName() const { return joinFieldName; }
} // namespace NES::QueryCompilation::PhysicalOperators