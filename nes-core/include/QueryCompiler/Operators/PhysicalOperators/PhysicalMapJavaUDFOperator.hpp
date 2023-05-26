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

#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPJAVAUDFOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPJAVAUDFOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace Catalogs::UDF {
class JavaUDFDescriptor;
using JavaUDFDescriptorPtr = std::shared_ptr<JavaUDFDescriptor>;

}// namespace Catalogs::UDF
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical Map Java Udf operator.
 */
class PhysicalMapJavaUDFOperator : public PhysicalUnaryOperator {
  public:
    /**
     * @brief Constructor for PhysicalMapJavaUDFOperator
     * @param id The identifier of this operator
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param javaUDFDescriptor The UDF descriptor for the Java-based UDF
     */
    PhysicalMapJavaUDFOperator(OperatorId id,
                               SchemaPtr inputSchema,
                               SchemaPtr outputSchema,
                               Catalogs::UDF::JavaUDFDescriptorPtr javaUDFDescriptor);
    /**
     * @brief Creates a new instance of PhysicalMapJavaUDFOperator
     * @param id The identifier of this operator
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param javaUDFDescriptor The UDF descriptor for the Java-based UDF
     * @return A new instance of PhysicalMapJavaUDFOperator
     */
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const Catalogs::UDF::JavaUDFDescriptorPtr& javaUDFDescriptor);

    /**
     * @brief Creates a new instance of PhysicalMapJavaUDFOperator with no specified operator ID
     * @param inputSchema The schema of the input data
     * @param outputSchema The schema of the output data
     * @param javaUDFDescriptor The UDF descriptor for the Java-based UDF
     * @return A new instance of PhysicalMapJavaUDFOperator
     */
    static PhysicalOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, Catalogs::UDF::JavaUDFDescriptorPtr javaUDFDescriptor);

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
     * @brief Returns the java udf descriptor of this map operator
     * @return FieldAssignmentExpressionNodePtr
     */
    Catalogs::UDF::JavaUDFDescriptorPtr getJavaUDFDescriptor();

  protected:
    const Catalogs::UDF::JavaUDFDescriptorPtr javaUDFDescriptor;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPJAVAUDFOPERATOR_HPP_