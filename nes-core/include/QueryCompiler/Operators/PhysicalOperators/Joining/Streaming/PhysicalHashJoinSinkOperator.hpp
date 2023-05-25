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

#ifndef NES_PHYSICALHASHJOINSINKOPERATOR_HPP
#define NES_PHYSICALHASHJOINSINKOPERATOR_HPP

#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
/**
 * @brief This class represents the physical hash stream join sink operator and gets translated to a HashJoinSink operator
 */
class PhysicalHashJoinSinkOperator : public PhysicalHashJoinOperator, public PhysicalBinaryOperator, public AbstractScanOperator {

  public:
    /**
     * @brief creates a PhysicalHashJoinSinkOperator with a provided operatorId
     * @param id
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     * @return PhysicalHashJoinSinkOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler);

    /**
     * @brief creates a PhysicalHashJoinSinkOperator that retrieves a new operatorId by calling method
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     * @return PhysicalHashJoinSinkOperator
     */
    static PhysicalOperatorPtr create(const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler);

    /**
     * @brief Constructor for a PhysicalHashJoinSinkOperator
     * @param id
     * @param leftSchema
     * @param rightSchema
     * @param outputSchema
     * @param operatorHandler
     */
    PhysicalHashJoinSinkOperator(OperatorId id,
                                 SchemaPtr leftSchema,
                                 SchemaPtr rightSchema,
                                 SchemaPtr outputSchema,
                                 Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr operatorHandler);

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
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif//NES_PHYSICALHASHJOINSINKOPERATOR_HPP
