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

#include <Operators/LogicalOperators/WindowJavaUDFLogicalOperatorNode.hpp>
#include <sstream>

namespace NES {
WindowJavaUDFLogicalOperatorNode::WindowJavaUDFLogicalOperatorNode(
    const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor,
    const Windowing::WindowTypePtr windowType,
    const Windowing::DistributionCharacteristicPtr distributionType,
    const std::vector<FieldAccessExpressionNodePtr> onKey,
    const uint64_t allowedLateness,
    const OriginId originId,
    const OperatorId id)
    : OperatorNode(id), JavaUDFLogicalOperator(javaUdfDescriptor, id), windowType(windowType), distributionType(distributionType),
      onKey(onKey), allowedLateness(allowedLateness), originId(originId) {}

std::string WindowJavaUDFLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WINDOW_JAVA_UDF(" << id << ")";
    return ss.str();
}

OperatorNodePtr WindowJavaUDFLogicalOperatorNode::copy() {
    auto copy = std::make_shared<WindowJavaUDFLogicalOperatorNode>(this->getJavaUDFDescriptor(),
                                                                   this->getWindowType(),
                                                                   this->getDistributionType(),
                                                                   this->getKeys(),
                                                                   this->getAllowedLateness(),
                                                                   this->getOriginId(),
                                                                   id);
    return copyInternal(copy);
}

OperatorNodePtr WindowJavaUDFLogicalOperatorNode::copyInternal(std::shared_ptr<WindowJavaUDFLogicalOperatorNode>& copy) {
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

bool WindowJavaUDFLogicalOperatorNode::equal(const NodePtr& other) const {
    return other->instanceOf<WindowJavaUDFLogicalOperatorNode>() && JavaUDFLogicalOperator::equal(other);
}

bool WindowJavaUDFLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<WindowJavaUDFLogicalOperatorNode>()->id;
}

Windowing::WindowTypePtr WindowJavaUDFLogicalOperatorNode::getWindowType() const { return windowType; }

Windowing::DistributionCharacteristicPtr WindowJavaUDFLogicalOperatorNode::getDistributionType() const {
    return distributionType;
}

const std::vector<FieldAccessExpressionNodePtr>& WindowJavaUDFLogicalOperatorNode::getKeys() const { return onKey; }

uint64_t WindowJavaUDFLogicalOperatorNode::getAllowedLateness() const { return allowedLateness; }

OriginId WindowJavaUDFLogicalOperatorNode::getOriginId() const { return originId; }

bool WindowJavaUDFLogicalOperatorNode::isKeyed() const { return !onKey.empty(); }
}// namespace NES