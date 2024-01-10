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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_OPENCLLOGICALOPERATORNODE_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_OPENCLLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/UDFs/UDFLogicalOperator.hpp>

namespace NES {

class OpenCLLogicalOperatorNode : public UDFLogicalOperator {

  public:
    OpenCLLogicalOperatorNode(Catalogs::UDF::JavaUdfDescriptorPtr javaUDFDescriptor, OperatorId id);

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

    [[nodiscard]] const std::string& getOpenClCode() const;

    void setOpenClCode(const std::string& openClCode);

    [[nodiscard]] size_t getDeviceId() const;

    void setDeviceId(const size_t deviceId);

    /**
     * Getter for the Java UDF descriptor.
     * @return The descriptor of the Java UDF used in the map operation.
     */
    Catalogs::UDF::JavaUDFDescriptorPtr getJavaUDFDescriptor() const;

  private:
    std::string openCLCode;
    size_t deviceId;
};
}// namespace NES
#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_OPENCLLOGICALOPERATORNODE_HPP_
