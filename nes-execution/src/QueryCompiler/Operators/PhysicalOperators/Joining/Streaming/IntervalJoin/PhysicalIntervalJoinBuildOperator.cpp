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

#include <Execution/Operators/Streaming/Join/IntervalJoin/IJOperatorHandler.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/IntervalJoin/PhysicalIntervalJoinBuildOperator.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr
PhysicalIntervalJoinBuildOperator::create(OperatorId id,
                                          StatisticId statisticId,
                                          const SchemaPtr& inputSchema,
                                          const SchemaPtr& otherSideInputSchema,
                                          const SchemaPtr& outputSchema,
                                          const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler,
                                          const JoinBuildSideType buildSide,
                                          TimestampField timeStampFieldThisSide,
                                          TimestampField timeStampFieldOtherSide) {
    return std::make_shared<PhysicalIntervalJoinBuildOperator>(id,
                                                               statisticId,
                                                               inputSchema,
                                                               otherSideInputSchema,
                                                               outputSchema,
                                                               operatorHandler,
                                                               buildSide,
                                                               std::move(timeStampFieldThisSide),
                                                               std::move(timeStampFieldOtherSide));
}
PhysicalOperatorPtr
PhysicalIntervalJoinBuildOperator::create(StatisticId statisticId,
                                          const SchemaPtr& inputSchema,
                                          const SchemaPtr& otherSideInputSchema,
                                          const SchemaPtr& outputSchema,
                                          const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler,
                                          const JoinBuildSideType buildSide,
                                          TimestampField timeStampFieldThisSide,
                                          TimestampField timeStampFieldOtherSide) {
    return create(getNextOperatorId(),
                  statisticId,
                  inputSchema,
                  otherSideInputSchema,
                  outputSchema,
                  operatorHandler,
                  buildSide,
                  std::move(timeStampFieldThisSide),
                  std::move(timeStampFieldOtherSide));
}

PhysicalIntervalJoinBuildOperator::PhysicalIntervalJoinBuildOperator(
    const OperatorId id,
    const StatisticId statisticId,
    const SchemaPtr& inputSchema,
    const SchemaPtr& otherSideInputSchema,
    const SchemaPtr& outputSchema,
    const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler,
    const JoinBuildSideType buildSide,
    TimestampField timeStampFieldThisSide,
    TimestampField timeStampFieldOtherSide)
    : Operator(id), PhysicalUnaryOperator(id, statisticId, inputSchema, outputSchema),
      timeStampFieldThisSide(std::move(timeStampFieldThisSide)), timeStampFieldOtherSide(timeStampFieldOtherSide),
      buildSide(buildSide), otherSideInputSchema(otherSideInputSchema), operatorHandler(operatorHandler) {}

std::string PhysicalIntervalJoinBuildOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalIntervalJoinBuildOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << "timeStampField: " << timeStampFieldThisSide << "\n";
    out << "buildSide: " << magic_enum::enum_name(buildSide);
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalIntervalJoinBuildOperator::copy() {
    auto result = create(id,
                         statisticId,
                         inputSchema,
                         otherSideInputSchema,
                         outputSchema,
                         operatorHandler,
                         buildSide,
                         timeStampFieldThisSide,
                         timeStampFieldOtherSide);
    result->addAllProperties(properties);
    return result;
}

JoinBuildSideType PhysicalIntervalJoinBuildOperator::getBuildSide() const { return buildSide; }

const TimestampField& PhysicalIntervalJoinBuildOperator::getTimeStampFieldThisSide() const { return timeStampFieldThisSide; }

const TimestampField& PhysicalIntervalJoinBuildOperator::getTimeStampFieldOtherSide() const { return timeStampFieldOtherSide; }

const Runtime::Execution::Operators::IJOperatorHandlerPtr& PhysicalIntervalJoinBuildOperator::getIJOperatorHandler() const {
    return operatorHandler;
}

const SchemaPtr& PhysicalIntervalJoinBuildOperator::getOtherSideInputSchema() const { return otherSideInputSchema; }

}// namespace NES::QueryCompilation::PhysicalOperators
