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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALNESTEDLOOPJOINPROBEOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALNESTEDLOOPJOINPROBEOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
/**
 * @brief This class represents the physical NLJ stream join probe operator and gets translated to a NestedLoopJoinProbe operator
 */
class PhysicalNestedLoopJoinProbeOperator : public PhysicalNestedLoopJoinOperator,
                                            public PhysicalBinaryOperator,
                                            public AbstractScanOperator {
  public:
    /**
     * @brief creates a PhysicalNestedLoopJoinProbeOperator with a provided operatorId
     * @param id
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param windowStartFieldName
     * @param windowEndFieldName
     * @param windowKeyFieldName
     * @param operatorHandler
     * @return PhysicalNestedLoopJoinProbeOperator
     */
    static PhysicalOperatorPtr create(const OperatorId id,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& joinFieldNameLeft,
                                      const std::string& joinFieldNameRight,
                                      const std::string& windowStartFieldName,
                                      const std::string& windowEndFieldName,
                                      const std::string& windowKeyFieldName,
                                      const Runtime::Execution::Operators::NLJOperatorHandlerPtr& operatorHandler);

    /**
     * @brief creates a PhysicalNestedLoopJoinProbeOperator that retrieves a new operatorId by calling method
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param windowStartFieldName
     * @param windowEndFieldName
     * @param windowKeyFieldName
     * @param operatorHandler
     * @return PhysicalNestedLoopJoinProbeOperator
     */
    static PhysicalOperatorPtr create(const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& joinFieldNameLeft,
                                      const std::string& joinFieldNameRight,
                                      const std::string& windowStartFieldName,
                                      const std::string& windowEndFieldName,
                                      const std::string& windowKeyFieldName,
                                      const Runtime::Execution::Operators::NLJOperatorHandlerPtr& operatorHandler);

    /**
     * @brief Constructor for a PhysicalNestedLoopJoinProbeOperator
     * @param id
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param windowStartFieldName
     * @param windowEndFieldName
     * @param windowKeyFieldName
     * @param operatorHandler
     */
    PhysicalNestedLoopJoinProbeOperator(OperatorId id,
                                        SchemaPtr leftSchema,
                                        SchemaPtr rightSchema,
                                        SchemaPtr outputSchema,
                                        const std::string& joinFieldNameLeft,
                                        const std::string& joinFieldNameRight,
                                        const std::string& windowStartFieldName,
                                        const std::string& windowEndFieldName,
                                        const std::string& windowKeyFieldName,
                                        Runtime::Execution::Operators::NLJOperatorHandlerPtr operatorHandler);

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
     * @return
     */
    const std::string& getJoinFieldNameRight() const;

    /**
     * @brief Getter for the window start field name
     * @return std::string
     */
    const std::string& getWindowStartFieldName() const;

    /**
     * @brief Getter for the window end field name
     * @return std::string
     */
    const std::string& getWindowEndFieldName() const;

    /**
     * @brief Getter for the window key field name
     * @return std::string
     */
    const std::string& getWindowKeyFieldName() const;

  protected:
    const std::string joinFieldNameLeft;
    const std::string joinFieldNameRight;
    const std::string windowStartFieldName;
    const std::string windowEndFieldName;
    const std::string windowKeyFieldName;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALNESTEDLOOPJOINPROBEOPERATOR_HPP_
