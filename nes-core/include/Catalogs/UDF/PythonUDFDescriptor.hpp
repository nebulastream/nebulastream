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
#ifndef NES_CORE_INCLUDE_CATALOGS_UDF_PYTHONUDFDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_CATALOGS_UDF_PYTHONUDFDESCRIPTOR_HPP_

#include <API/Schema.hpp>
#include <Catalogs/UDF/UDFDescriptor.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <memory>
#include <string>

namespace NES::Catalogs::UDF {

class PythonUDFDescriptor;
using PythonUDFDescriptorPtr = std::shared_ptr<PythonUDFDescriptor>;

class PythonUDFDescriptor : public UDFDescriptor {
  public:
    PythonUDFDescriptor(const std::string& functionName,
                        std::string& functionString,
                        const SchemaPtr inputSchema,
                        const SchemaPtr outputSchema);

    static PythonUDFDescriptorPtr create(const std::string& functionName,
                                         std::string& functionString,
                                         const SchemaPtr inputSchema,
                                         const SchemaPtr outputSchema) {
        return std::make_shared<PythonUDFDescriptor>(functionName, functionString, inputSchema, outputSchema);
    }

    /**
     * @brief Return the fully-qualified class name of the class implementing the UDF.
     * @return Fully-qualified class name of the class implementing the UDF.
     */
    const std::string& getFunctionString() const { return functionString; }

    /**
     * @brief Return the output schema of the map UDF operation.
     *
     * The output schema must correspond to the return type of the UDF method.
     *
     * @return A SchemaPtr instance describing the output schema of the UDF method.
     */
    const SchemaPtr& getOutputSchema() const { return outputSchema; }

    /**
     * @brief Return the input schema of the map UDF operation.
     *
     * The input schema must correspond to the input type of the UDF method.
     *
     * @return A SchemaPtr instance describing the input schema of the UDF method.
     */
    const SchemaPtr& getInputSchema() const { return inputSchema; }

    /**
     * @brief Set the input schema of the map UDF operation.
     *
     * The input schema must correspond to the input type of the UDF method.
     *
     * @param inputSchema A SchemaPtr instance describing the input schema of the UDF method.
     */
    void setInputSchema(const SchemaPtr& inputSchema);

    /**
     * Compare to Python UDF descriptors.
     *
     * @param other The other PythonUDFDescriptor in the comparison.
     * @return True, if both PythonUdfDescriptors are the same, i.e., same UDF class and method name,
     * same serialized instance (state), and same byte code list; False, otherwise.
     */
    bool operator==(const PythonUDFDescriptor& other) const;


  private:
    const std::string functionName;
    const std::string functionString;
    SchemaPtr inputSchema;
    const SchemaPtr outputSchema;
};
}// namespace NES::Catalogs::UDF
#endif// NES_CORE_INCLUDE_CATALOGS_UDF_PYTHONUDFDESCRIPTOR_HPP_
#endif// NAUTILUS_PYTHON_UDF_ENABLED