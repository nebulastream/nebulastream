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
#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <Operators/LogicalOperators/FlatMapJavaUDFLogicalOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>

namespace NES {

FlatMapJavaUDFLogicalOperatorNode::FlatMapJavaUDFLogicalOperatorNode(const Catalogs::UDF::JavaUDFDescriptorPtr javaUDFDescriptor,
                                                                     OperatorId id)
    : OperatorNode(id), JavaUDFLogicalOperator(javaUDFDescriptor, id) {}

std::string FlatMapJavaUDFLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "FLATMAP_JAVA_UDF(" << id << ")";
    return ss.str();
}

OperatorNodePtr FlatMapJavaUDFLogicalOperatorNode::copy() {
    auto copy = std::make_shared<FlatMapJavaUDFLogicalOperatorNode>(this->getJavaUDFDescriptor(), id);
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

bool FlatMapJavaUDFLogicalOperatorNode::equal(const NodePtr& other) const {
    return other->instanceOf<FlatMapJavaUDFLogicalOperatorNode>() && JavaUDFLogicalOperator::equal(other);
}

bool FlatMapJavaUDFLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<FlatMapJavaUDFLogicalOperatorNode>()->id;
}

}// namespace NES