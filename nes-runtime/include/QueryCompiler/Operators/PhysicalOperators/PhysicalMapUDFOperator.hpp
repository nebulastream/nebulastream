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

#ifndef NES_RUNTIME_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPUDFOPERATOR_HPP_
#define NES_RUNTIME_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPUDFOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace Catalogs::UDF {
class UDFDescriptor;
using UDFDescriptorPtr = std::shared_ptr<UDFDescriptor>;

}// namespace Catalogs::UDF
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical Map Udf operator.
 */
class PhysicalMapUDFOperator : public PhysicalUnaryOperator {
  public:
    /**
     * @brief Constructor for PhysicalMapUDFOperator
     * @param id The identifier of this operator
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param udfDescriptor The UDF descriptor
     */
    PhysicalMapUDFOperator(OperatorId id,
                           const SchemaPtr& inputSchema,
                           const SchemaPtr& outputSchema,
                           const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor);
    /**
     * @brief Creates a new instance of PhysicalMapUDFOperator
     * @param id The identifier of this operator
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param udfDescriptor The UDF descriptor
     * @return A new instance of PhysicalMapUDFOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor);

    /**
     * @brief Creates a new instance of PhysicalMapUDFOperator with no specified operator ID
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param udfDescriptor The UDF descriptor
     * @return A new instance of PhysicalMapUDFOperator
     */
    static PhysicalOperatorPtr
    create(const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, const Catalogs::UDF::UDFDescriptorPtr udfDescriptor);

    /**
     * @brief Returns a string representation of this operator
     * @return A string representation of this operator
     */
    std::string toString() const override;

    /**
     * @brief Creates a copy of this operator node
     * @return A copy of this operator node
     */
    OperatorNodePtr copy() override;

    /**
     * @brief Returns the udf descriptor of this map operator
     * @return FieldAssignmentExpressionNodePtr
     */
    Catalogs::UDF::UDFDescriptorPtr getUDFDescriptor();

  protected:
    const Catalogs::UDF::UDFDescriptorPtr udfDescriptor;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_RUNTIME_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPUDFOPERATOR_HPP_
