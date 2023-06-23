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

#include <Operators/LogicalOperators/JavaUDFLogicalOperator.hpp>

namespace NES {

class OpenCLLogicalOperatorNode : public JavaUDFLogicalOperator {

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
     * @brief: get device id
     */
    std::string getDeviceId();

    /**
     * @brief: set device type
     * @param deviceType
     */
    void setDeviceType(const std::string& deviceType);

    /**
     * @brief: get device type
     */
    std::string getDeviceType();

    /**
     * @brief: set device model name
     * @param modelName
     */
    void setDeviceModelName(const std::string& modelName);

    /**
     * @brief: get device model name
     */
    std::string getDeviceModelName();

    /**
     * @brief: set device memory information
     * @param deviceMemory
     */
    void setDeviceMemory(const uint16_t& deviceMemory);

    /**
     * @brief: get device memory information
     */
    uint16_t getDeviceMemory();

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

  private:
    std::string openCLCode;
    std::string deviceId;
    std::string deviceType;
    std::string modelName;
    uint16_t modelMemory;
};
}// namespace NES
#endif//NES_OPENCLLOGICALOPERATORNODE_HPP
