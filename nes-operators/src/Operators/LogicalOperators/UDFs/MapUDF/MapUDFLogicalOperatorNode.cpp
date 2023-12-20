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
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>

namespace NES {

MapUDFLogicalOperatorNode::MapUDFLogicalOperatorNode(const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor, OperatorId id)
    : OperatorNode(id), UDFLogicalOperator(udfDescriptor, id) {}

std::string MapUDFLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP_UDF(" << id << ")";
    return ss.str();
}

OperatorNodePtr MapUDFLogicalOperatorNode::copy() {
    auto copy = std::make_shared<MapUDFLogicalOperatorNode>(this->getUDFDescriptor(), id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool MapUDFLogicalOperatorNode::equal(const NodePtr& other) const {
    return other->instanceOf<MapUDFLogicalOperatorNode>() && UDFLogicalOperator::equal(other);
}

bool MapUDFLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<MapUDFLogicalOperatorNode>()->id;
}

}// namespace NES