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

#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALFLATMAPJAVAUDFOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALFLATMAPJAVAUDFOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace Catalogs::UDF {
class JavaUDFDescriptor;
using JavaUdfDescriptorPtr = std::shared_ptr<JavaUDFDescriptor>;
}// namespace Catalogs::UDF
namespace QueryCompilation::PhysicalOperators {

/**
 * @brief Physical FlatMap Java Udf operator.
 */
class PhysicalFlatMapJavaUDFOperator : public PhysicalUnaryOperator {
  public:
    /**
     * @brief Constructor for PhysicalFlatMapJavaUDFOperator
     * @param id The identifier of this operator
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param javaUdfDescriptor The UDF descriptor for the Java-based UDF
     */
    PhysicalFlatMapJavaUDFOperator(OperatorId id,
                                   const SchemaPtr& inputSchema,
                                   const SchemaPtr& outputSchema,
                                   const Catalogs::UDF::JavaUDFDescriptorPtr& javaUdfDescriptor);
    /**
     * @brief Creates a new instance of PhysicalFlatMapJavaUDFOperator
     * @param id The identifier of this operator
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param javaUdfDescriptor The UDF descriptor for the Java-based UDF
     * @return A new instance of PhysicalFlatMapJavaUDFOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Catalogs::UDF::JavaUDFDescriptorPtr& javaUdfDescriptor);

    /**
     * @brief Creates a new instance of PhysicalFlatMapJavaUDFOperator with no specified operator ID
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param javaUdfDescriptor The UDF descriptor for the Java-based UDF
     * @return A new instance of PhysicalFlatMapJavaUDFOperator
     */
    static PhysicalOperatorPtr create(const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Catalogs::UDF::JavaUDFDescriptorPtr& javaUdfDescriptor);

    /**
     * @brief Returns the java udf descriptor of this map operator
     * @return FieldAssignmentExpressionNodePtr
     */
    Catalogs::UDF::JavaUDFDescriptorPtr getJavaUDFDescriptor();

    std::string toString() const override;
    OperatorNodePtr copy() override;

  protected:
    const Catalogs::UDF::JavaUDFDescriptorPtr javaUDFDescriptor;
};
}// namespace QueryCompilation::PhysicalOperators
}// namespace NES

#endif// NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALFLATMAPJAVAUDFOPERATOR_HPP_