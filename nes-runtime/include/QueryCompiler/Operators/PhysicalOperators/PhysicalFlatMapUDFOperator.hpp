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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALFLATMAPUDFOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALFLATMAPUDFOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace Catalogs::UDF {
class UDFDescriptor;
using UDFDescriptorPtr = std::shared_ptr<UDFDescriptor>;
}// namespace Catalogs::UDF
namespace QueryCompilation::PhysicalOperators {

/**
 * @brief Physical FlatMap UDF operator.
 */
class PhysicalFlatMapUDFOperator : public PhysicalUnaryOperator {
  public:
    /**
     * @brief Constructor for PhysicalFlatMapUDFOperator
     * @param id The identifier of this operator
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param udfDescriptor The UDF descriptor
     */
    PhysicalFlatMapUDFOperator(OperatorId id,
                               const SchemaPtr& inputSchema,
                               const SchemaPtr& outputSchema,
                               const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor);
    /**
     * @brief Creates a new instance of PhysicalFlatMapUDFOperator
     * @param id The identifier of this operator
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param udfDescriptor The UDF descriptor
     * @return A new instance of PhysicalFlatMapUDFOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor);

    /**
     * @brief Creates a new instance of PhysicalFlatMapUDFOperator with no specified operator ID
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param udfDescriptor The UDF descriptor
     * @return A new instance of PhysicalFlatMapUDFOperator
     */
    static PhysicalOperatorPtr
    create(const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, const Catalogs::UDF::UDFDescriptorPtr udfDescriptor);

    /**
     * @brief Returns the udf descriptor of this map operator
     * @return FieldAssignmentExpressionNodePtr
     */
    Catalogs::UDF::UDFDescriptorPtr getUDFDescriptor();

    std::string toString() const override;
    OperatorNodePtr copy() override;

  protected:
    const Catalogs::UDF::UDFDescriptorPtr udfDescriptor;
};
}// namespace QueryCompilation::PhysicalOperators
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALFLATMAPUDFOPERATOR_HPP_
