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

#ifndef NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALCOUNTMINBUILDOPERATOR_HPP_
#define NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALCOUNTMINBUILDOPERATOR_HPP_

#include <Execution/Operators/Statistics/CountMinOperatorHandler.hpp>
#include <Operators/LogicalOperators/Statistics/CountMinDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::Experimental::Statistics {

class CountMinOperatorHandler;
using CountMinOperatorHandlerPtr = std::shared_ptr<CountMinOperatorHandler>;

/**
 * @brief the class of physical CountMin sketches which are then lowered down to the actual operator
 */
class PhysicalCountMinBuildOperator : public QueryCompilation::PhysicalOperators::PhysicalUnaryOperator,
                                      public QueryCompilation::PhysicalOperators::AbstractEmitOperator {

  public:
    /**
     * @brief the constructor of the physicalCountMin sketches
     * @param operatorId the id of the operator
     * @param windowStatisticDescriptor the descriptor, describing the count min sketch
     * @param inputSchema the input schema of the sketch
     * @param outputSchema the output schema of the sketch
     * @param operatorHandler the state of the sketch
     */
    PhysicalCountMinBuildOperator(OperatorId operatorId,
                                  WindowStatisticDescriptorPtr windowStatisticDescriptor,
                                  SchemaPtr inputSchema,
                                  SchemaPtr outputSchema,
                                  const CountMinOperatorHandlerPtr& operatorHandler);

    /**
     * @param operatorId the id of the operator
     * @param windowStatisticDescriptor the descriptor, describing the count min sketch
     * @param inputSchema the input schema of the sketch
     * @param outputSchema the output schema of the sketch
     * @param operatorHandler the state of the sketch
     * @return a PhysicalOperatorPtr
     */
    static QueryCompilation::PhysicalOperators::PhysicalOperatorPtr
    create(OperatorId operatorId,
           const WindowStatisticDescriptorPtr& windowStatisticDescriptor,
           const SchemaPtr& inputSchema,
           const SchemaPtr& outputSchema,
           const CountMinOperatorHandlerPtr& operatorHandler);

    /**
     * @param windowStatisticDescriptor the descriptor, describing the count min sketch
     * @param inputSchema the input schema of the sketch
     * @param outputSchema the output schema of the sketch
     * @param operatorHandler the state of the sketch
     * @return a PhysicalOperatorPtr
     */
    static QueryCompilation::PhysicalOperators::PhysicalOperatorPtr
    create(const WindowStatisticDescriptorPtr& windowStatisticDescriptor,
           const SchemaPtr& inputSchema,
           const SchemaPtr& outputSchema,
           const CountMinOperatorHandlerPtr& operatorHandler);

    /**
     * @return a string representation of the physicalCountMin sketch
     */
    std::string toString() const override;

    /**
     * @return a copy of that operator
     */
    OperatorNodePtr copy() override;

    /**
     * @return returns the descriptor of the sketch
     */
    const WindowStatisticDescriptorPtr& getDescriptor() const;

    /**
     * @return returns the state of the count Min sketch
     */
    const CountMinOperatorHandlerPtr& getCountMinOperatorHandler() const;

  private:
    WindowStatisticDescriptorPtr descriptor;
    CountMinOperatorHandlerPtr countMinOperatorHandler;
};
}// namespace NES::Experimental::Statistics
#endif//NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALCOUNTMINBUILDOPERATOR_HPP_
