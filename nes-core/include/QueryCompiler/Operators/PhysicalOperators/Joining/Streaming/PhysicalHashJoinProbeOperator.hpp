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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALHASHJOINPROBEOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALHASHJOINPROBEOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
/**
 * @brief This class represents the physical hash stream join probe operator and gets translated to a HashJoinProbe operator
 */
class PhysicalHashJoinProbeOperator : public PhysicalHashJoinOperator, public PhysicalBinaryOperator, public AbstractScanOperator {

  public:
    /**
     * @brief creates a PhysicalHashJoinProbeOperator with a provided operatorId
     * @param id
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param operatorHandler
     * @return PhysicalHashJoinProbeOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& joinFieldNameLeft,
                                      const std::string& joinFieldNameRight,
                                      const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler);

    /**
     * @brief creates a PhysicalHashJoinProbeOperator that retrieves a new operatorId by calling method
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param operatorHandler
     * @return PhysicalHashJoinProbeOperator
     */
    static PhysicalOperatorPtr create(const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& joinFieldNameLeft,
                                      const std::string& joinFieldNameRight,
                                      const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler);

    /**
     * @brief Constructor for a PhysicalHashJoinProbeOperator
     * @param id
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param operatorHandler
     */
    PhysicalHashJoinProbeOperator(OperatorId id,
                                 const SchemaPtr& leftSchema,
                                 const SchemaPtr& rightSchema,
                                 const SchemaPtr& outputSchema,
                                 const std::string& joinFieldNameLeft,
                                 const std::string& joinFieldNameRight,
                                 const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler);

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
     * @brief getter for right join fiel name
     * @return
     */
    const std::string& getJoinFieldNameRight() const;

  protected:
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_STREAMING_PHYSICALHASHJOINPROBEOPERATOR_HPP_
