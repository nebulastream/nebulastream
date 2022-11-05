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
#include <numeric>

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
    NES_ASSERT(children.size() == 1, "MapUdfLogicalOperatorNode should have exactly 1 child.");
    // Infer query signatures for child operator.
    auto child = children[0]->as<LogicalOperatorNode>();
    child->inferStringSignature();
    // Infer signature for this operator based on the UDF metadata (class name and UDF method), the serialized instance,
    // and the byte code list. We can ignore the schema information because it is determined by the UDF method signature.
    auto elementHash = std::hash<Catalogs::UDF::JavaSerializedInstance::value_type>{};
    // Hash the contents of a byte array (i.e., the serialized instance and the byte code of a class)
    // based on the hashes of the individual elements.
    auto charArrayHashHelper = [&elementHash](std::size_t h, char v) {
        return h = h * 31 + elementHash(v);
    };
    // Compute hashed value of the UDF instance.
    auto& instance = javaUdfDescriptor->getSerializedInstance();
    auto instanceHash = std::accumulate(instance.begin(), instance.end(), instance.size(), charArrayHashHelper);
    // Compute hashed value of the UDF byte code list.
    auto stringHash = std::hash<std::string>{};
    auto& byteCodeList = javaUdfDescriptor->getByteCodeList();
    auto byteCodeListHash = std::accumulate(
        byteCodeList.begin(), byteCodeList.end(), byteCodeList.size(),
        [&stringHash, &charArrayHashHelper](std::size_t h, Catalogs::UDF::JavaUdfByteCodeList::value_type v) {
            auto& className = v.first;
            h = h * 31 + stringHash(className);
            auto& byteCode = v.second;
            h = h * 31 + std::accumulate(byteCode.begin(), byteCode.end(), byteCode.size(), charArrayHashHelper);
            return h;
    });
    auto signatureStream = std::stringstream{};
    signatureStream << "MAP_JAVA_UDF("
                    << javaUdfDescriptor->getClassName() << "." << javaUdfDescriptor->getMethodName()
                    << ", instance=" << instanceHash
                    << ", byteCode=" << byteCodeListHash << ")"
                    << "." << *child->getHashBasedSignature().begin()->second.begin();
    auto signature = signatureStream.str();
    hashBasedSignature[stringHash(signature)] = {signature};
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