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
#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <Operators/LogicalOperators/MapJavaUdfLogicalOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <numeric>
#include <sstream>

namespace NES {

MapJavaUdfLogicalOperatorNode::MapJavaUdfLogicalOperatorNode(const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor,
                                                             OperatorId id)
    : OperatorNode(id), JavaUdfLogicalOperator(javaUdfDescriptor, id) {}


std::string MapJavaUdfLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP_JAVA_UDF(" << id << ")";
    return ss.str();
}

OperatorNodePtr MapJavaUdfLogicalOperatorNode::copy() {
    auto copy = std::make_shared<MapJavaUdfLogicalOperatorNode>(this->getJavaUdfDescriptor(), id);
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

bool MapJavaUdfLogicalOperatorNode::equal(const NodePtr& other) const {
    // Explicit check here, so the cast using as throws no exception.
    return other->instanceOf<MapJavaUdfLogicalOperatorNode>() && JavaUdfLogicalOperator::equal(other);
}

bool MapJavaUdfLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<MapJavaUdfLogicalOperatorNode>()->id;
}

}// namespace NES