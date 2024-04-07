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

#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinProbeOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr
PhysicalStreamJoinProbeOperator::create(OperatorId id,
                                        StatisticId statisticId,
                                        const SchemaPtr& leftSchema,
                                        const SchemaPtr& rightSchema,
                                        const SchemaPtr& outputSchema,
                                        const std::string& joinFieldNameLeft,
                                        const std::string& joinFieldNameRight,
                                        const std::string& windowStartFieldName,
                                        const std::string& windowEndFieldName,
                                        const std::string& windowKeyFieldName,
                                        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                        QueryCompilation::StreamJoinStrategy joinStrategy,
                                        QueryCompilation::WindowingStrategy windowingStrategy) {
    return std::make_shared<PhysicalStreamJoinProbeOperator>(id,
                                                             statisticId,
                                                             leftSchema,
                                                             rightSchema,
                                                             outputSchema,
                                                             joinFieldNameLeft,
                                                             joinFieldNameRight,
                                                             windowStartFieldName,
                                                             windowEndFieldName,
                                                             windowKeyFieldName,
                                                             operatorHandler,
                                                             joinStrategy,
                                                             windowingStrategy);
}

PhysicalOperatorPtr
PhysicalStreamJoinProbeOperator::create(StatisticId statisticId,
                                        const SchemaPtr& leftSchema,
                                        const SchemaPtr& rightSchema,
                                        const SchemaPtr& outputSchema,
                                        const std::string& joinFieldNameLeft,
                                        const std::string& joinFieldNameRight,
                                        const std::string& windowStartFieldName,
                                        const std::string& windowEndFieldName,
                                        const std::string& windowKeyFieldName,
                                        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                        QueryCompilation::StreamJoinStrategy joinStrategy,
                                        QueryCompilation::WindowingStrategy windowingStrategy) {
    return create(getNextOperatorId(),
                  statisticId,
                  leftSchema,
                  rightSchema,
                  outputSchema,
                  joinFieldNameLeft,
                  joinFieldNameRight,
                  windowStartFieldName,
                  windowEndFieldName,
                  windowKeyFieldName,
                  operatorHandler,
                  joinStrategy,
                  windowingStrategy);
}

PhysicalStreamJoinProbeOperator::PhysicalStreamJoinProbeOperator(
    OperatorId id,
    StatisticId statisticId,
    const SchemaPtr& leftSchema,
    const SchemaPtr& rightSchema,
    const SchemaPtr& outputSchema,
    const std::string& joinFieldNameLeft,
    const std::string& joinFieldNameRight,
    const std::string& windowStartFieldName,
    const std::string& windowEndFieldName,
    const std::string& windowKeyFieldName,
    const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
    QueryCompilation::StreamJoinStrategy joinStrategy,
    QueryCompilation::WindowingStrategy windowingStrategy)
    : Operator(id), PhysicalStreamJoinOperator(operatorHandler, joinStrategy, windowingStrategy),
      PhysicalBinaryOperator(id, statisticId, leftSchema, rightSchema, outputSchema), joinFieldNameLeft(joinFieldNameLeft),
      joinFieldNameRight(joinFieldNameRight), windowMetaData(windowStartFieldName, windowEndFieldName, windowKeyFieldName) {}

std::string PhysicalStreamJoinProbeOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalStreamJoinProbeOperator:\n";
    out << PhysicalBinaryOperator::toString();
    out << "joinFieldNameLeft: " << joinFieldNameLeft << "\n";
    out << "joinFieldNameRight: " << joinFieldNameRight << "\n";
    out << "windowStartFieldName: " << windowMetaData.windowStartFieldName << "\n";
    out << "windowEndFieldName: " << windowMetaData.windowEndFieldName << "\n";
    out << "windowKeyFieldName: " << windowMetaData.windowKeyFieldName;
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalStreamJoinProbeOperator::copy() {
    return create(id,
                  statisticId,
                  leftInputSchema,
                  rightInputSchema,
                  outputSchema,
                  joinFieldNameLeft,
                  joinFieldNameRight,
                  windowMetaData.windowStartFieldName,
                  windowMetaData.windowEndFieldName,
                  windowMetaData.windowKeyFieldName,
                  joinOperatorHandler,
                  getJoinStrategy(),
                  getWindowingStrategy());
}

const std::string& PhysicalStreamJoinProbeOperator::getJoinFieldNameLeft() const { return joinFieldNameLeft; }
const std::string& PhysicalStreamJoinProbeOperator::getJoinFieldNameRight() const { return joinFieldNameRight; }
const Runtime::Execution::Operators::WindowMetaData& PhysicalStreamJoinProbeOperator::getWindowMetaData() const {
    return windowMetaData;
}

Runtime::Execution::Operators::JoinSchema PhysicalStreamJoinProbeOperator::getJoinSchema() {
    return Runtime::Execution::Operators::JoinSchema(getLeftInputSchema(), getRightInputSchema(), getOutputSchema());
}

}// namespace NES::QueryCompilation::PhysicalOperators