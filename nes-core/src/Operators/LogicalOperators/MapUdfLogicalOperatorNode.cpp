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

#include <Operators/LogicalOperators/MapUdfLogicalOperatorNode.hpp>
#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <sstream>

namespace NES {

MapUdfLogicalOperatorNode::MapUdfLogicalOperatorNode(OperatorId id, const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), javaUdfDescriptor(javaUdfDescriptor) {}

Catalogs::UDF::JavaUdfDescriptorPtr MapUdfLogicalOperatorNode::getJavaUdfDescriptor() const {
    return javaUdfDescriptor;
}

bool MapUdfLogicalOperatorNode::inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) {
    // Set the input schema.
    if (!LogicalUnaryOperatorNode::inferSchema(typeInferencePhaseContext)) {
        return false;
    }
    // The output schema of this operation is determined by the Java UDF.
    outputSchema = javaUdfDescriptor->getOutputSchema();
    return true;
}

void MapUdfLogicalOperatorNode::inferStringSignature() {

}

std::string MapUdfLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP_JAVA_UDF(" << id << ")";
    return ss.str();
}

OperatorNodePtr MapUdfLogicalOperatorNode::copy() {
    return std::make_shared<MapUdfLogicalOperatorNode>(*this);
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
bool MapUdfLogicalOperatorNode::equal(const NodePtr& other) const {
    return false;
}
#pragma clang diagnostic pop

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
bool MapUdfLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return false;
}
#pragma clang diagnostic pop

}