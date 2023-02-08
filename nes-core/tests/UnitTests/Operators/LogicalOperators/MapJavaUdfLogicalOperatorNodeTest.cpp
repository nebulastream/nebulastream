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

#include <API/Schema.hpp>
#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/MapJavaUdfLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
#include <Util/JavaUdfDescriptorBuilder.hpp>
#include <Util/SchemaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <string>

using namespace std::string_literals;

namespace NES {

class MapJavaUdfLogicalOperatorNodeTest : public Testing::NESBaseTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("MapJavaUdfLogicalOperatorNodeTest", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(MapJavaUdfLogicalOperatorNodeTest, InferSchema) {
    // Create a JavaUdfDescriptor with a specific schema.
    auto outputSchema = std::make_shared<Schema>()->addField("outputAttribute", DataTypeFactory::createBoolean());
    auto javaUdfDescriptor = Catalogs::UDF::JavaUdfDescriptorBuilder{}.setOutputSchema(outputSchema).build();
    // Create a MapUdfLogicalOperatorNode with the JavaUdfDescriptor.
    auto mapUdfLogicalOperatorNode = std::make_shared<MapJavaUdfLogicalOperatorNode>(javaUdfDescriptor, 1);
    // Create a SourceLogicalOperatorNode with a source schema
    // and add it as a child to the MapUdfLogicalOperatorNode to infer the input schema.
    auto inputSchema = std::make_shared<Schema>()->addField("inputAttribute", DataTypeFactory::createUInt64());
    auto sourceDescriptor = std::make_shared<SchemaSourceDescriptor>(inputSchema);
    mapUdfLogicalOperatorNode->addChild(std::make_shared<SourceLogicalOperatorNode>(sourceDescriptor, 2));
    // After calling inferSchema on the MapUdfLogicalOperatorNode,
    // the output schema of the node should be the output schema of the JavaUdfDescriptor,
    // and the input schema should be the schema of the source.
    auto typeInferencePhaseContext =
        Optimizer::TypeInferencePhaseContext{Catalogs::Source::SourceCatalogPtr(), Catalogs::UDF::UdfCatalogPtr()};
    mapUdfLogicalOperatorNode->inferSchema(typeInferencePhaseContext);
    ASSERT_TRUE(mapUdfLogicalOperatorNode->getInputSchema()->equals(inputSchema));
    ASSERT_TRUE(mapUdfLogicalOperatorNode->getOutputSchema()->equals(outputSchema));
}

TEST_F(MapJavaUdfLogicalOperatorNodeTest, InferStringSignature) {
    // Create a MapUdfLogicalOperatorNode with a JavaUdfDescriptor and a source as a child.
    auto javaUdfDescriptor = Catalogs::UDF::JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    auto mapUdfLogicalOperatorNode = std::make_shared<MapJavaUdfLogicalOperatorNode>(javaUdfDescriptor, 1);
    auto child = std::make_shared<SourceLogicalOperatorNode>(
        std::make_shared<SchemaSourceDescriptor>(
            std::make_shared<Schema>()->addField("inputAttribute", DataTypeFactory::createUInt64())),
        2);
    mapUdfLogicalOperatorNode->addChild(child);
    // After calling inferStringSignature, the map returned by `getHashBasesStringSignature` contains an entry.
    mapUdfLogicalOperatorNode->inferStringSignature();
    auto hashBasedSignature = mapUdfLogicalOperatorNode->getHashBasedSignature();
    ASSERT_TRUE(hashBasedSignature.size() == 1);
    // The signature ends with the string signature of the child.
    auto& signature = *hashBasedSignature.begin()->second.begin();
    auto& childSignature = *child->getHashBasedSignature().begin()->second.begin();
    NES_DEBUG(signature);
    ASSERT_TRUE(signature.ends_with("." + childSignature));
}

// Regression test for https://github.com/nebulastream/nebulastream/issues/3484
// Setup: A MapJavaUdfLogicalOperatorNode n1 has a parent p1 (e.g., a sink).
// We create a copies p2 of p1 and n2 of n1.
// The bug: When we try to add p2 as a parent to n2, the parent is not added because there already exists a parent with the same
// operator ID.
// Cause: n2 is a shallow copy of n1, so it retains the list of parents of n1 with the same IDs.
TEST_F(MapJavaUdfLogicalOperatorNodeTest, AddParentToCopy) {
    // given: Create MapJavaUdfLogicalOperatorNode with a parent.
    auto n1 = LogicalOperatorFactory::createMapJavaUdfLogicalOperator(Catalogs::UDF::JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor());
    auto p1 = LogicalOperatorFactory::createSinkOperator(NullOutputSinkDescriptor::create());
    n1->addParent(p1);
    // when: Create copies of n1 and p1 and add p2 as parent of n2. They should not be in a parent-child relationship.
    auto n2 = n1->copy();
    auto p2 = p1->copy();
    EXPECT_TRUE(p2->getChildren().empty());
    EXPECT_TRUE(n2->getParents().empty());
    // Check that we can add p2 as a parent of n2.
    n2->addParent(p2);
    EXPECT_TRUE(p2->getChildWithOperatorId(n1->getId()) == n2);
    EXPECT_FALSE(n2->getParents().empty());
    EXPECT_TRUE(n2->getParents()[0] == p2);
}

}// namespace NES
