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

#ifndef NES_NES_RUNTIME_SRC_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALRESERVOIRSAMPLEBUILDOPERATOR_HPP_
#define NES_NES_RUNTIME_SRC_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALRESERVOIRSAMPLEBUILDOPERATOR_HPP_

#include <Operators/LogicalOperators/Statistics/ReservoirSampleDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::Experimental::Statistics {

/**
 * @brief the class of physical reservoir samples which are then lowered down to the actual operator
 */
class PhysicalReservoirSampleBuildOperator : public QueryCompilation::PhysicalOperators::PhysicalUnaryOperator,
                                             public QueryCompilation::PhysicalOperators::AbstractEmitOperator  {
  public:
    /**
     * @brief the constructor of the physicalReservoirSample
     * @param operatorId the id of the operator
     * @param windowStatisticDescriptor the descriptor, describing the reservoir
     * @param inputSchema the input schema of the reservoir
     * @param outputSchema the output schema of the reservoir
     * @return a PhysicalOperatorPtr
     */
    PhysicalReservoirSampleBuildOperator(OperatorId operatorId,
                                         WindowStatisticDescriptorPtr windowStatisticDescriptor,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema);

    /**
     * @param operatorId the id of the operator
     * @param windowStatisticDescriptor the descriptor, describing the reservoir
     * @param inputSchema the input schema of the reservoir
     * @param outputSchema the output schema of the reservoir
     * @return a PhysicalOperatorPtr
     */
    static QueryCompilation::PhysicalOperators::PhysicalOperatorPtr create(OperatorId operatorId,
                                                                           const WindowStatisticDescriptorPtr& windowStatisticDescriptor,
                                                                           const SchemaPtr& inputSchema,
                                                                           const SchemaPtr& outputSchema);

    /**
     * @param windowStatisticDescriptor the descriptor, describing the reservoir
     * @param inputSchema the input schema of the reservoir
     * @param outputSchema the output schema of the reservoir
     * @return a PhysicalOperatorPtr
     */
    static QueryCompilation::PhysicalOperators::PhysicalOperatorPtr create(const WindowStatisticDescriptorPtr& windowStatisticDescriptor,
                                                                           const SchemaPtr& inputSchema,
                                                                           const SchemaPtr& outputSchema);

    /**
     * @return a string representation of the physicalCountMin sketch
     */
    std::string toString() const override;

    /**
     * @return a copy of that operator
     */
    OperatorNodePtr copy() override;

    /**
     * @return returns the descriptor of the reservoir
     */
    const WindowStatisticDescriptorPtr& getDescriptor() const;

  private:
    WindowStatisticDescriptorPtr descriptor;
};

}// namespace NES::Experimental::Statistics

#endif//NES_NES_RUNTIME_SRC_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALRESERVOIRSAMPLEBUILDOPERATOR_HPP_
