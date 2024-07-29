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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINPROBEOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINPROBEOPERATOR_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
/**
 * @brief This class represents the physical stream join probe operator and gets translated to a join probe operator
 */
class PhysicalStreamJoinProbeOperator : public PhysicalStreamJoinOperator,
                                        public PhysicalBinaryOperator,
                                        public AbstractScanOperator {

  public:
    /**
     * @brief creates a PhysicalStreamJoinProbeOperator with a provided operatorId
     * @param id
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     * @param windowStartFieldName: name for the field name that stores the window start value
     * @param windowEndFieldName: name for the field name that stores the window end value
     * @return PhysicalStreamJoinProbeOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      StatisticId statisticId,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const ExpressionNodePtr joinExpression,
                                      const std::string& windowStartFieldName,
                                      const std::string& windowEndFieldName,
                                      const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                      QueryCompilation::StreamJoinStrategy joinStrategy,
                                      QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Creates a PhysicalStreamJoinProbeOperator that retrieves a new operatorId by calling method
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     * @param windowStartFieldName
     * @param windowEndFieldName
     * @return PhysicalStreamJoinProbeOperator
     */
    static PhysicalOperatorPtr create(StatisticId statisticId,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      ExpressionNodePtr joinExpression,
                                      const std::string& windowStartFieldName,
                                      const std::string& windowEndFieldName,
                                      const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                      QueryCompilation::StreamJoinStrategy joinStrategy,
                                      QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Constructor for a PhysicalStreamJoinProbeOperator
     * @param id
     * @param statisticId: represents the unique identifier of components that we can track statistics for
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     */
    PhysicalStreamJoinProbeOperator(OperatorId id,
                                    StatisticId statisticId,
                                    const SchemaPtr& leftSchema,
                                    const SchemaPtr& rightSchema,
                                    const SchemaPtr& outputSchema,
                                    ExpressionNodePtr joinExpression,
                                    const std::string& windowStartFieldName,
                                    const std::string& windowEndFieldName,
                                    const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                    QueryCompilation::StreamJoinStrategy joinStrategy,
                                    QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Creates a string containing the name of this physical operator
     * @return String
     */
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Performs a deep copy of this physical operator
     * @return OperatorPtr
     */
    OperatorPtr copy() override;

    /**
     * @brief Getter for the window meta data
     * @return WindowMetaData
     */
    const Runtime::Execution::Operators::WindowMetaData& getWindowMetaData() const;

    /**
     * @brief Getter for the join schema
     * @return JoinSchema
     */
    Runtime::Execution::Operators::JoinSchema getJoinSchema();

    /**
     * @brief Getter for joinExpression
     * @return joinExpression
     */
    ExpressionNodePtr getJoinExpression() const;

  protected:
    ExpressionNodePtr joinExpression;
    const Runtime::Execution::Operators::WindowMetaData windowMetaData;
};
}// namespace NES::QueryCompilation::PhysicalOperators
#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINPROBEOPERATOR_HPP_
