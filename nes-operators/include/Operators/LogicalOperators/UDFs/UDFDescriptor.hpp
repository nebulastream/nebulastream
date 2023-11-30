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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_UDFS_UDFDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_UDFS_UDFDESCRIPTOR_HPP_

#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <memory>
#include <string>
#include <utility>

namespace NES::Catalogs::UDF {

class UDFDescriptor;
using UDFDescriptorPtr = std::shared_ptr<UDFDescriptor>;

class UDFDescriptor {
  public:
    explicit UDFDescriptor(const std::string& methodName, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema);

    virtual ~UDFDescriptor() = default;

    /**
    * @brief Return the name of the UDF method.
    * @return The name of the UDF method.udf
    */
    [[nodiscard]] const std::string& getMethodName() const { return methodName; }

    /**
    * @brief Retrieve the return type of the UDF
    * @return A DataType pointer for the UDF return type
    */
    [[nodiscard]] DataTypePtr getReturnType() const { return returnType; }

    /**
     * @brief Return the output schema of the UDF operation.
     *
     * The output schema must correspond to the return type of the UDF method.
     *
     * @return A SchemaPtr instance describing the output schema of the UDF method.
     */
    const SchemaPtr& getOutputSchema() const { return outputSchema; }

    /**
     * @brief Return the input schema of the UDF operation.
     *
     * The input schema must correspond to the input type of the UDF method.
     *
     * @return A SchemaPtr instance describing the input schema of the UDF method.
     */
    const SchemaPtr& getInputSchema() const { return inputSchema; }

    /**
     * @brief Set the input schema of the UDF operation.
     *
     * The input schema must correspond to the input type of the UDF method.
     *
     * @param inputSchema A SchemaPtr instance describing the input schema of the UDF method.
     */
    void setInputSchema(const SchemaPtr& inputSchema);

    virtual std::stringstream generateInferStringSignature() = 0;

    template<class UDFDescriptor>
    static std::shared_ptr<UDFDescriptor> as(UDFDescriptorPtr ptr) {
        return std::dynamic_pointer_cast<UDFDescriptor>(ptr);
    }

    /**
     * @brief Checks if the current udf descriptor is of type UDFDescriptor
     * @tparam NodeType
     * @return bool true if node is of NodeType
     */
    template<class UDFDescriptor>
    bool instanceOf() {
        if (dynamic_cast<UDFDescriptor*>(this)) {
            return true;
        };
        return false;
    };

    /**
     * Compare to UDF descriptors.
     *
     * @param other The other UDFDescriptor in the comparison.
     * @return True, if both UDFDescriptors are the same, i.e., same UDF class and method name,
     * same serialized instance (state), and same byte code list; False, otherwise.
     */
    bool operator==(const UDFDescriptor& other) const;

  private:
    const std::string methodName;
    const DataTypePtr returnType;
    SchemaPtr inputSchema;
    const SchemaPtr outputSchema;
};
}// namespace NES::Catalogs::UDF
#endif  // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_UDFS_UDFDESCRIPTOR_HPP_
