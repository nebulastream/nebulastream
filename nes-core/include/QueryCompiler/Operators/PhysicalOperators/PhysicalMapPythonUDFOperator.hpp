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
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
        namespace Catalogs::UDF {
        class PythonUDFDescriptor;
        using PythonUDFDescriptorPtr = std::shared_ptr<PythonUDFDescriptor>;

        }// namespace Catalogs::UDF
        namespace QueryCompilation {
        namespace PhysicalOperators {

        /**
* @brief Physical Map Python Udf operator.
*/
        class PhysicalMapPythonUDFOperator : public PhysicalUnaryOperator {
          public:
            /**
 * @brief Constructor for PhysicalMapPythonUDFOperator
 * @param id The identifier of this operator
 * @param inputSchema The schema of the input data
 * @param outputSchema The schema of the output data
 * @param UDFDescriptor The UDF descriptor for the Python-based UDF
 */
            PhysicalMapPythonUDFOperator(OperatorId id,
                                       SchemaPtr inputSchema,
                                       SchemaPtr outputSchema,
                                       Catalogs::UDF::PythonUDFDescriptorPtr pythonUDFDescriptor);
            /**
 * @brief Creates a new instance of PhysicalMapPythonUDFOperator
 * @param id The identifier of this operator
 * @param inputSchema The schema of the input data
 * @param outputSchema The schema of the output data
 * @param pythonUDFDescriptor The UDF descriptor for the Python-based UDF
 * @return A new instance of PhysicalMapPythonUDFOperator
 */
            static PhysicalOperatorPtr create(OperatorId id,
                                              const SchemaPtr& inputSchema,
                                              const SchemaPtr& outputSchema,
                                              const Catalogs::UDF::PythonUDFDescriptorPtr& pythonUDFDescriptor);

            /**
 * @brief Creates a new instance of PhysicalMapPythonUDFOperator with no specified operator ID
 * @param inputSchema The schema of the input data
 * @param outputSchema The schema of the output data
 * @param pythonUDFDescriptor The UDF descriptor for the Python-based UDF
 * @return A new instance of PhysicalMapPythonUDFOperator
 */
            static PhysicalOperatorPtr
            create(SchemaPtr inputSchema, SchemaPtr outputSchema, Catalogs::UDF::PythonUDFDescriptorPtr pythonUDFDescriptor);

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
 * @brief Returns the Python udf descriptor of this map operator
 * @return FieldAssignmentExpressionNodePtr
 */
            Catalogs::UDF::PythonUDFDescriptorPtr getPythonUDFDescriptor();

protected:
    const Catalogs::UDF::PythonUDFDescriptorPtr pythonUDFDescriptor;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES
#endif// NAUTILUS_PYTHON_UDF_ENABLED