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

#ifndef NES_PYTHONUDFDESCRIPTORBUILDER_H
#define NES_PYTHONUDFDESCRIPTORBUILDER_H

#include <string>

using namespace std::string_literals;

#include "Common/DataTypes/DataTypeFactory.hpp"
#include <Catalogs/UDF/PythonUDFDescriptor.hpp>

namespace NES::Catalogs::UDF {

/**
 * Utility class to create default and non-default Python UDF descriptors for testing.
 *
 * The class PythonUDFDescriptor performs a number of checks in its constructor.
 * Creating the required inputs everytime a test needs a Python UDF descriptor leads to code repetition.
 */
class PythonUDFDescriptorBuilder {
  public:
    /**
     * Create a new builder for a PythonUDFDescriptor with valid default values for the fields required by the PythonUDFDescriptor.
     */
    PythonUDFDescriptorBuilder() = default;

    /**
     * @return A Python UDF descriptor with the fields either set to default values or with explicitly specified values in setters.
     */
    PythonUDFDescriptorPtr build();

    /**
     * Set the function name of the Python UDF descriptor.
     * @param newFunctionName The function name of the Python UDF descriptor.
     * @return The PythonUDFDescriptorBuilder instance.
     */
    PythonUDFDescriptorBuilder& setFunctionName(const std::string& newFunctionName);

    /**
     * Set the function name of the Python UDF descriptor.
     * @param newFunctionName The function name of the Python UDF descriptor.
     * @return The PythonUDFDescriptorBuilder instance.
     */
    PythonUDFDescriptorBuilder& setFunctionString(const std::string& newFunctionString);

    /**
     * Set the input schema of the Python UDF descriptor.
     * @param newInputSchema The input schema of the Python UDF descriptor.
     * @return The PythonUDFDescriptorBuilder instance.
     */
    PythonUDFDescriptorBuilder& setInputSchema(const SchemaPtr& newInputSchema) {
        this->inputSchema = newInputSchema;
        return *this;
    }

    /**
     * Set the output schema of the Python UDF descriptor.
     * @param newOutputSchema The output schema of the Python UDF descriptor.
     * @return The PythonUDFDescriptorBuilder instance.
     */
    PythonUDFDescriptorBuilder& setOutputSchema(const SchemaPtr& newOutputSchema);

    /**
     * Set the class name of the input type of the UDF function.
     * @param newInputClassName The class name of the input type of the UDF function.
     * @return The PythonUDFDescriptorBuilder instance.
     */
    PythonUDFDescriptorBuilder& setInputClassName(const std::string newInputClassName);

    /**
     * Create a default Python UDF descriptor that can be used in tests.
     * @return A Python UDF descriptor instance.
     */
    static PythonUDFDescriptorPtr createDefaultPythonUDFDescriptor();

  private:
    std::string functionName = "udf_function";
    std::string functionString = "def udf_function(x):\n\ty = x + 10\n\treturn y\n";
    SchemaPtr inputSchema = std::make_shared<Schema>()->addField("inputAttribute", DataTypeFactory::createUInt64());
    SchemaPtr outputSchema = std::make_shared<Schema>()->addField("outputAttribute", DataTypeFactory::createUInt64());

};

}// namespace NES::Catalogs::UDF

#endif//NES_PYTHONUDFDESCRIPTORBUILDER_H
