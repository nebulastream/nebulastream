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
#include <numeric>
#include <sstream>

namespace NES {

MapJavaUdfLogicalOperatorNode::MapJavaUdfLogicalOperatorNode(const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor,
                                                             OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), javaUdfDescriptor(javaUdfDescriptor) {}

Catalogs::UDF::JavaUdfDescriptorPtr MapJavaUdfLogicalOperatorNode::getJavaUdfDescriptor() const { return javaUdfDescriptor; }

bool MapJavaUdfLogicalOperatorNode::inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) {
    // Set the input schema.
    if (!LogicalUnaryOperatorNode::inferSchema(typeInferencePhaseContext)) {
        return false;
    }
    // The output schema of this operation is determined by the Java UDF.
    outputSchema = javaUdfDescriptor->getOutputSchema();
    //Update output schema by changing the qualifier and corresponding attribute names
    auto newQualifierName = inputSchema->getQualifierNameForSystemGeneratedFields() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    for (auto& field : outputSchema->fields) {
        //Extract field name without qualifier
        auto fieldName = field->getName();
        //Add new qualifier name to the field and update the field name
        field->setName(newQualifierName + fieldName);
    }
    return true;
}

void MapJavaUdfLogicalOperatorNode::inferStringSignature() {
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
    auto byteCodeListHash =
        std::accumulate(byteCodeList.begin(),
                        byteCodeList.end(),
                        byteCodeList.size(),
                        [&stringHash, &charArrayHashHelper](std::size_t h, Catalogs::UDF::JavaUdfByteCodeList::value_type v) {
                            auto& className = v.first;
                            h = h * 31 + stringHash(className);
                            auto& byteCode = v.second;
                            h = h * 31 + std::accumulate(byteCode.begin(), byteCode.end(), byteCode.size(), charArrayHashHelper);
                            return h;
                        });
    auto signatureStream = std::stringstream{};
    signatureStream << "MAP_JAVA_UDF(" << javaUdfDescriptor->getClassName() << "." << javaUdfDescriptor->getMethodName()
                    << ", instance=" << instanceHash << ", byteCode=" << byteCodeListHash << ")"
                    << "." << *child->getHashBasedSignature().begin()->second.begin();
    auto signature = signatureStream.str();
    hashBasedSignature[stringHash(signature)] = {signature};
}

std::string MapJavaUdfLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP_JAVA_UDF(" << id << ")";
    return ss.str();
}

OperatorNodePtr MapJavaUdfLogicalOperatorNode::copy() { return std::make_shared<MapJavaUdfLogicalOperatorNode>(*this); }

bool MapJavaUdfLogicalOperatorNode::equal(const NodePtr& other) const {
    // Explicit check here, so the cast using as throws no exception.
    if (!other->instanceOf<MapJavaUdfLogicalOperatorNode>()) {
        return false;
    }
    return *javaUdfDescriptor == *other->as<MapJavaUdfLogicalOperatorNode>()->javaUdfDescriptor;
}

bool MapJavaUdfLogicalOperatorNode::isIdentical(const NodePtr& other) const {
    return equal(other) && id == other->as<MapJavaUdfLogicalOperatorNode>()->id;
}

}// namespace NES