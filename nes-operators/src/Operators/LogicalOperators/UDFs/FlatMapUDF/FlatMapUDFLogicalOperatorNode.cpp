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
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>

namespace NES {

FlatMapUDFLogicalOperatorNode::FlatMapUDFLogicalOperatorNode(const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor, OperatorId id)
    : OperatorNode(id), UDFLogicalOperator(udfDescriptor, id) {}

std::string FlatMapUDFLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "FLATMAP_UDF(" << id << ")";
    return ss.str();
}

OperatorNodePtr FlatMapUDFLogicalOperatorNode::copy() {
    auto copy = std::make_shared<FlatMapUDFLogicalOperatorNode>(this->getUDFDescriptor(), id);
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

bool FlatMapUDFLogicalOperatorNode::equal(const NodePtr& other) const {
    return other->instanceOf<FlatMapUDFLogicalOperatorNode>() && UDFLogicalOperator::equal(other);
}

bool FlatMapUDFLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<FlatMapUDFLogicalOperatorNode>()->id;
}

}// namespace NES