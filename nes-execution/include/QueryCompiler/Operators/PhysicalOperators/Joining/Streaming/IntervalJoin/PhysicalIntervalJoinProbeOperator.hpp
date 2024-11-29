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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_INTERVALJOIN_PHYSICALINTERVALJOINPROBEOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_INTERVALJOIN_PHYSICALINTERVALJOINPROBEOPERATOR_HPP_

#include <Execution/Operators/Streaming/Join/IntervalJoin/IJOperatorHandler.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

class PhysicalIntervalJoinProbeOperator : public PhysicalBinaryOperator, public AbstractScanOperator {

  public:
    /**
     * @brief creates a PhysicalIntervalJoinProbeOperator with the provided operatorId
     * @return PhysicalIntervalJoinProbeOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      StatisticId statisticId,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const ExpressionNodePtr joinExpression,
                                      const std::string& intervalStartField,
                                      const std::string& intervalEndField,
                                      TimestampField timestampField,
                                      const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler);

    /**
     * @brief Creates a PhysicalIntervalJoinProbeOperator that retrieves a new operatorId by calling method
     * @return PhysicalIntervalJoinProbeOperator
     */
    static PhysicalOperatorPtr create(StatisticId statisticId,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const ExpressionNodePtr joinExpression,
                                      const std::string& intervalStartField,
                                      const std::string& intervalEndField,
                                      TimestampField timestampField,
                                      const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler);

    /**
     * @brief Constructor for a PhysicalIntervalJoinProbeOperator
     */
    PhysicalIntervalJoinProbeOperator(OperatorId id,
                                      StatisticId statisticId,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const ExpressionNodePtr joinExpression,
                                      const std::string& intervalStartField,
                                      const std::string& intervalEndField,
                                      TimestampField timestampField,
                                      const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler);

    [[nodiscard]] std::string toString() const override;

    OperatorPtr copy() override;

    /**
     * @brief getter for join expression
     * @return
     */
    ExpressionNodePtr getJoinExpression() const;

    /**
     * @brief getter for left join field name
     * @return
     */
    const std::string& getIntervalStartField() const;

    /**
     * @brief getter for right join field name
     * @return std::string
     */
    const std::string& getIntervalEndField() const;

    /**
     * @brief getter for right time stamp field
     * @return TimestampField
     */
    TimestampField getTimeStampField() const;

    /**
     * @brief getter for the IJ operator handler
     * @return IJOperatorHandler
     */
    const Runtime::Execution::Operators::IJOperatorHandlerPtr& getIJOperatorHandler() const;

  protected:
    ExpressionNodePtr joinExpression;
    const std::string intervalStartField;
    const std::string intervalEndField;
    TimestampField timestampField;
    Runtime::Execution::Operators::IJOperatorHandlerPtr operatorHandler;
};

}// namespace NES::QueryCompilation::PhysicalOperators
#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_INTERVALJOIN_PHYSICALINTERVALJOINPROBEOPERATOR_HPP_
