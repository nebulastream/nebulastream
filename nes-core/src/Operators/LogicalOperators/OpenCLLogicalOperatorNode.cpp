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

OpenCLLogicalOperatorNode::OpenCLLogicalOperatorNode(Catalogs::UDF::JavaUdfDescriptorPtr javaUDFDescriptor, OperatorId id)
    : OperatorNode(id), UDFLogicalOperator(javaUDFDescriptor, id) {}

std::string OpenCLLogicalOperatorNode::toString() const {
    auto javaUDFDescriptor = getUDFDescriptor()->as<Catalogs::UDF::JavaUDFDescriptor>(getUDFDescriptor());
    return "OPENCL_LOGICAL_OPERATOR(" + javaUDFDescriptor->getClassName() + "." + javaUDFDescriptor->getMethodName() + ")";
}

OperatorNodePtr OpenCLLogicalOperatorNode::copy() {
    auto javaUDFDescriptor = getUDFDescriptor()->as<Catalogs::UDF::JavaUDFDescriptor>(getUDFDescriptor());
    auto copy = std::make_shared<OpenCLLogicalOperatorNode>(javaUDFDescriptor, id);
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
        && *getUDFDescriptor() == *other->as<OpenCLLogicalOperatorNode>()->getUDFDescriptor();
}

bool OpenCLLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<OpenCLLogicalOperatorNode>()->id;
}

const std::string& OpenCLLogicalOperatorNode::getOpenClCode() const { return openCLCode; }

void OpenCLLogicalOperatorNode::setOpenClCode(const std::string& openClCode) { openCLCode = openClCode; }

const std::string& OpenCLLogicalOperatorNode::getDeviceId() const { return deviceId; }

void OpenCLLogicalOperatorNode::setDeviceId(const std::string& deviceId) { OpenCLLogicalOperatorNode::deviceId = deviceId; }

Catalogs::UDF::JavaUDFDescriptorPtr OpenCLLogicalOperatorNode::getJavaUDFDescriptor() const {
    return udfDescriptor->as<Catalogs::UDF::JavaUDFDescriptor>(udfDescriptor);
}

}// namespace NES
