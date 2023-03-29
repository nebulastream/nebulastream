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

#include <Operators/LogicalOperators/WindowJavaUdfLogicalOperatorNode.hpp>
#include <sstream>

namespace NES {
WindowJavaUdfLogicalOperatorNode::WindowJavaUdfLogicalOperatorNode(
    const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor,
    const Windowing::WindowTypePtr windowType,
    const Windowing::DistributionCharacteristicPtr distributionType,
    const std::vector<FieldAccessExpressionNodePtr> onKey,
    const uint64_t allowedLateness,
    const OriginId originId,
    const OperatorId id)
    : OperatorNode(id), JavaUdfLogicalOperator(javaUdfDescriptor, id), windowType(windowType), distributionType(distributionType),
      onKey(onKey), allowedLateness(allowedLateness), originId(originId) {}

std::string WindowJavaUdfLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WINDOW_JAVA_UDF(" << id << ")";
    return ss.str();
}

OperatorNodePtr WindowJavaUdfLogicalOperatorNode::copy() {
    auto copy = std::make_shared<WindowJavaUdfLogicalOperatorNode>(this->getJavaUdfDescriptor(),
                                                                   this->getWindowType(),
                                                                   this->getDistributionType(),
                                                                   this->getKeys(),
                                                                   this->getAllowedLateness(),
                                                                   this->getOriginId(),
                                                                   id);
    return copyInternal(copy);
}

OperatorNodePtr WindowJavaUdfLogicalOperatorNode::copyInternal(std::shared_ptr<WindowJavaUdfLogicalOperatorNode>& copy) {
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

bool WindowJavaUdfLogicalOperatorNode::equal(const NodePtr& other) const {
    return other->instanceOf<WindowJavaUdfLogicalOperatorNode>() && JavaUdfLogicalOperator::equal(other);
}

bool WindowJavaUdfLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<WindowJavaUdfLogicalOperatorNode>()->id;
}

Windowing::WindowTypePtr WindowJavaUdfLogicalOperatorNode::getWindowType() const { return windowType; }

Windowing::DistributionCharacteristicPtr WindowJavaUdfLogicalOperatorNode::getDistributionType() const {
    return distributionType;
}

const std::vector<FieldAccessExpressionNodePtr>& WindowJavaUdfLogicalOperatorNode::getKeys() const { return onKey; }

uint64_t WindowJavaUdfLogicalOperatorNode::getAllowedLateness() const { return allowedLateness; }

OriginId WindowJavaUdfLogicalOperatorNode::getOriginId() const { return originId; }

bool WindowJavaUdfLogicalOperatorNode::isKeyed() const { return !onKey.empty(); }
}// namespace NES