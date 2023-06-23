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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <numeric>
#include <utility>

namespace NES {

OpenCLLogicalOperatorNode::OpenCLLogicalOperatorNode(Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor, OperatorId id)
    : OperatorNode(id), JavaUDFLogicalOperator(javaUdfDescriptor, id) {}

void OpenCLLogicalOperatorNode::setDeviceId(const std::string& deviceId) { this->deviceId = deviceId; }

void OpenCLLogicalOperatorNode::setOpenCLCode(const std::string& openCLCode) { this->openCLCode = openCLCode; }

std::string OpenCLLogicalOperatorNode::toString() const {
    return "OPENCL_LOGICAL_OPERATOR(" + getJavaUDFDescriptor()->getClassName() + "." + getJavaUDFDescriptor()->getMethodName()
        + ")";
}

OperatorNodePtr OpenCLLogicalOperatorNode::copy() {
    auto copy = std::make_shared<OpenCLLogicalOperatorNode>(this->getJavaUDFDescriptor(), id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool OpenCLLogicalOperatorNode::equal(const NodePtr& other) const {
    return other->instanceOf<OpenCLLogicalOperatorNode>()
        && *getJavaUDFDescriptor() == *other->as<OpenCLLogicalOperatorNode>()->getJavaUDFDescriptor();
}

bool OpenCLLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<OpenCLLogicalOperatorNode>()->id;
}

}// namespace NES
