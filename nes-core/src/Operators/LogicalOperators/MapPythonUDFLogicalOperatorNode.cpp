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
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
#include <API/AttributeField.hpp>
#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
#include <Operators/LogicalOperators/MapPythonUDFLogicalOperatorNode.hpp>
#include <sstream>

namespace NES {

MapPythonUDFLogicalOperatorNode::MapPythonUDFLogicalOperatorNode(const Catalogs::UDF::PythonUDFDescriptorPtr& pythonUDFDescriptor,
                                                                         OperatorId id)
                : OperatorNode(id), PythonUDFLogicalOperator(pythonUDFDescriptor, id) {}

std::string MapPythonUDFLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP_PYTHON_UDF(" << id << ")";
    return ss.str();
}

OperatorNodePtr MapPythonUDFLogicalOperatorNode::copy() {
    auto copy = std::make_shared<MapPythonUDFLogicalOperatorNode>(this->getPythonUDFDescriptor(), id);
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

bool MapPythonUDFLogicalOperatorNode::equal(const NodePtr& other) const {
    return other->instanceOf<MapPythonUDFLogicalOperatorNode>() && PythonUDFLogicalOperator::equal(other);
}

bool MapPythonUDFLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<MapPythonUDFLogicalOperatorNode>()->id;
}
}// namespace NES
#endif// NAUTILUS_PYTHON_UDF_ENABLED