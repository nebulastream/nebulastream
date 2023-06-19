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

#ifndef NES_OPENCLLOGICALOPERATORNODE_HPP
#define NES_OPENCLLOGICALOPERATORNODE_HPP

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>

namespace NES {

namespace Catalogs::UDF {
class JavaUDFDescriptor;
using JavaUdfDescriptorPtr = std::shared_ptr<JavaUDFDescriptor>;
}// namespace Catalogs::UDF

class OpenCLLogicalOperatorNode : public LogicalUnaryOperatorNode {

  public:
    OpenCLLogicalOperatorNode(Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor, OperatorId id);

    /**
     * @brief: set openCLCode
     * @param openCLCode
     */
    void setOpenCLCode(const std::string& openCLCode);

    /**
     * @brief: set device id
     * @param deviceId
     */
    void setDeviceId(const std::string& deviceId);

    /**
     * Get Java UDF descriptor
     * @return Java UDF descriptor
     */
    Catalogs::UDF::JavaUDFDescriptorPtr getJavaUDFDescriptor();

    /**
     * @see Node#toString
     */
    std::string toString() const override;

    /**
     * @see OperatorNode#copy
     */
    OperatorNodePtr copy() override;

    /**
     * @see Node#equal
     *
     * Two OpenCLLogicalOperatorNode are equal when the wrapped JavaUDFDescriptor are equal.
     */
    [[nodiscard]] bool equal(const NodePtr& other) const override;

    /**
     * @see Node#isIdentical
     */
    [[nodiscard]] bool isIdentical(const NodePtr& other) const override;

    void inferStringSignature() override;

    bool inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) override;

  private:
    Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor;
    std::string openCLCode;
    std::string deviceId;
};
}// namespace NES
#endif//NES_OPENCLLOGICALOPERATORNODE_HPP
