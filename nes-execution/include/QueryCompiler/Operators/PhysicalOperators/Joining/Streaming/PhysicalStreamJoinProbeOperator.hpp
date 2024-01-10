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
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param operatorHandler
     * @param windowStartFieldName: name for the field name that stores the window start value
     * @param windowEndFieldName: name for the field name that stores the window end value
     * @param windowKeyFieldName: name for the join key field, e.g., how the field name is called that stores the predicate value
     * @return PhysicalStreamJoinProbeOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
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
                                      QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Creates a PhysicalStreamJoinProbeOperator that retrieves a new operatorId by calling method
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param operatorHandler
     * @param windowStartFieldName
     * @param windowEndFieldName
     * @param windowKeyFieldName
     * @return PhysicalStreamJoinProbeOperator
     */
    static PhysicalOperatorPtr create(const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& joinFieldNameLeft,
                                      const std::string& joinFieldNameRight,
                                      const std::string& windowStartFieldName,
                                      const std::string& windowEndFieldName,
                                      const std::string& windowKeyFieldName,
                                      const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
                                      QueryCompilation::StreamJoinStrategy joinStrategy,
                                      QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Constructor for a PhysicalStreamJoinProbeOperator
     * @param id
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param operatorHandler
     */
    PhysicalStreamJoinProbeOperator(OperatorId id,
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
                                    QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Creates a string containing the name of this physical operator
     * @return String
     */
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Performs a deep copy of this physical operator
     * @return OperatorNodePtr
     */
    OperatorNodePtr copy() override;

    /**
     * @brief getter for left join field name
     * @return
     */
    const std::string& getJoinFieldNameLeft() const;

    /**
     * @brief getter for right join field name
     * @return std::string
     */
    const std::string& getJoinFieldNameRight() const;

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

  protected:
    const std::string joinFieldNameLeft;
    const std::string joinFieldNameRight;
    const Runtime::Execution::Operators::WindowMetaData windowMetaData;
};
}// namespace NES::QueryCompilation::PhysicalOperators
#endif // NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALSTREAMJOINPROBEOPERATOR_HPP_
