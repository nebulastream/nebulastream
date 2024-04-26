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

#include <Operators/Operator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr
PhysicalStreamJoinBuildOperator::create(OperatorId id,
                                        StatisticId statisticId,
                                        const SchemaPtr& inputSchema,
                                        const SchemaPtr& outputSchema,
                                        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                        const JoinBuildSideType buildSide,
                                        TimestampField timeStampField,
                                        const std::string& joinFieldName,
                                        QueryCompilation::StreamJoinStrategy joinStrategy,
                                        QueryCompilation::WindowingStrategy windowingStrategy) {
    return std::make_shared<PhysicalStreamJoinBuildOperator>(id,
                                                             statisticId,
                                                             inputSchema,
                                                             outputSchema,
                                                             operatorHandler,
                                                             buildSide,
                                                             std::move(timeStampField),
                                                             joinFieldName,
                                                             joinStrategy,
                                                             windowingStrategy);
}
PhysicalOperatorPtr
PhysicalStreamJoinBuildOperator::create(StatisticId statisticId,
                                        const SchemaPtr& inputSchema,
                                        const SchemaPtr& outputSchema,
                                        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                        const JoinBuildSideType buildSide,
                                        TimestampField timeStampField,
                                        const std::string& joinFieldName,
                                        QueryCompilation::StreamJoinStrategy joinStrategy,
                                        QueryCompilation::WindowingStrategy windowingStrategy) {
    return create(getNextOperatorId(),
                  statisticId,
                  inputSchema,
                  outputSchema,
                  operatorHandler,
                  buildSide,
                  std::move(timeStampField),
                  joinFieldName,
                  joinStrategy,
                  windowingStrategy);
}

PhysicalStreamJoinBuildOperator::PhysicalStreamJoinBuildOperator(
    const OperatorId id,
    const StatisticId statisticId,
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
    const JoinBuildSideType buildSide,
    TimestampField timeStampField,
    const std::string& joinFieldName,
    QueryCompilation::StreamJoinStrategy joinStrategy,
    QueryCompilation::WindowingStrategy windowingStrategy)
    : Operator(id), PhysicalStreamJoinOperator(operatorHandler, joinStrategy, windowingStrategy),
      PhysicalUnaryOperator(id, statisticId, inputSchema, outputSchema), timeStampField(std::move(timeStampField)),
      joinFieldName(joinFieldName), buildSide(buildSide) {}

std::string PhysicalStreamJoinBuildOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalStreamJoinBuildOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << "timeStampField: " << timeStampField << "\n";
    out << "joinFieldName: " << joinFieldName << "\n";
    out << "buildSide: " << magic_enum::enum_name(buildSide);
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalStreamJoinBuildOperator::copy() {
    auto result = create(id,
                         statisticId,
                         inputSchema,
                         outputSchema,
                         joinOperatorHandler,
                         buildSide,
                         timeStampField,
                         joinFieldName,
                         getJoinStrategy(),
                         getWindowingStrategy());
    result->addAllProperties(properties);
    return result;
}

JoinBuildSideType PhysicalStreamJoinBuildOperator::getBuildSide() const { return buildSide; }

const TimestampField& PhysicalStreamJoinBuildOperator::getTimeStampField() const { return timeStampField; }

const std::string& PhysicalStreamJoinBuildOperator::getJoinFieldName() const { return joinFieldName; }

}// namespace NES::QueryCompilation::PhysicalOperators