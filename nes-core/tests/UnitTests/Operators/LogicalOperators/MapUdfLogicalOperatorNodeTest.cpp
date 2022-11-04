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

#include <NesBaseTest.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/MapUdfLogicalOperatorNode.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <string>
#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Util/SchemaSourceDescriptor.hpp>

using namespace std::string_literals;

namespace NES {

class MapUdfLogicalOperatorNodeTest : public Testing::NESBaseTest {
  protected:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapUdfLogicalOperatorNodeTest", NES::LogLevel::LOG_DEBUG);
    }
};

TEST_F(MapUdfLogicalOperatorNodeTest, InferSchema) {
    // Create a JavaUdfDescriptor with a specific schema.
    auto outputSchema = std::make_shared<Schema>()->addField("outputAttribute", DataTypeFactory::createBoolean());
    auto javaUdfDescriptor = std::make_shared<Catalogs::UDF::JavaUdfDescriptor>(
        "some_class"s,
        "some_method"s,
        Catalogs::UDF::JavaSerializedInstance {1},
        Catalogs::UDF::JavaUdfByteCodeList {{"some_class"s, {1}}},
        outputSchema);
    // Create a MapUdfLogicalOperatorNode with the JavaUdfDescriptor.
    auto mapUdfLogicalOperatorNode = std::make_shared<MapUdfLogicalOperatorNode>(1, javaUdfDescriptor);
    // Create a SourceLogicalOperatorNode with a source schema
    // and add it as a child to the MapUdfLogicalOperatorNode to infer the input schema.
    auto inputSchema = std::make_shared<Schema>()->addField("inputAttribute", DataTypeFactory::createUInt64());
    auto sourceDescriptor = std::make_shared<SchemaSourceDescriptor>(inputSchema);
    mapUdfLogicalOperatorNode->addChild(std::make_shared<SourceLogicalOperatorNode>(sourceDescriptor, 2));
    // After calling inferSchema on the MapUdfLogicalOperatorNode,
    // the output schema of the node should be the output schema of the JavaUdfDescriptor,
    // and the input schema should be the schema of the source.
    auto typeInferencePhaseContext = Optimizer::TypeInferencePhaseContext{Catalogs::Source::SourceCatalogPtr(), Catalogs::UDF::UdfCatalogPtr()};
    mapUdfLogicalOperatorNode->inferSchema(typeInferencePhaseContext);
    ASSERT_TRUE(mapUdfLogicalOperatorNode->getInputSchema()->equals(inputSchema));
    ASSERT_TRUE(mapUdfLogicalOperatorNode->getOutputSchema()->equals(outputSchema));
}

}