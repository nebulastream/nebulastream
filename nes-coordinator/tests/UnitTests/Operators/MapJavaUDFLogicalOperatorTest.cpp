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
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperator.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/SchemaSourceDescriptor.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <string>

using namespace std::string_literals;

namespace NES {

class MapJavaUDFLogicalOperatorTest : public Testing::BaseUnitTest {
  protected:
    static void SetUpTestCase() { NES::Logger::setupLogging("MapJavaUDFLogicalOperatorTest", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(MapJavaUDFLogicalOperatorTest, InferSchema) {
    // Create a JavaUDFDescriptor with a specific schema.
    auto outputSchema = std::make_shared<Schema>()->addField("outputAttribute", DataTypeFactory::createBoolean());
    auto javaUdfDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder{}.setOutputSchema(outputSchema).build();

    // Create a MapUdfLogicalOperator with the JavaUDFDescriptor.
    auto mapUdfLogicalOperator = std::make_shared<MapUDFLogicalOperator>(javaUdfDescriptor, 1);

    // Create a SourceLogicalOperator with a source schema
    // and add it as a child to the MapUdfLogicalOperator to infer the input schema.
    const std::string sourceName = "sourceName";
    auto inputSchema = std::make_shared<Schema>()->addField(sourceName + "$inputAttribute", DataTypeFactory::createUInt64());
    auto sourceDescriptor = std::make_shared<SchemaSourceDescriptor>(inputSchema);
    mapUdfLogicalOperator->addChild(std::make_shared<SourceLogicalOperator>(sourceDescriptor, 2));

    // After calling inferSchema on the MapUdfLogicalOperator, the input schema should be the schema of the source.
    mapUdfLogicalOperator->inferSchema();
    ASSERT_TRUE(mapUdfLogicalOperator->getInputSchema()->equals(inputSchema));

    // The output schema of the operator should be prefixed with source name.
    auto expectedOutputSchema =
        std::make_shared<Schema>()->addField(sourceName + "$outputAttribute", DataTypeFactory::createBoolean());
    ASSERT_TRUE(mapUdfLogicalOperator->getOutputSchema()->equals(expectedOutputSchema));
}

TEST_F(MapJavaUDFLogicalOperatorTest, InferStringSignature) {
    // Create a MapUdfLogicalOperator with a JavaUDFDescriptor and a source as a child.
    auto javaUDFDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor();
    auto mapUdfLogicalOperator = std::make_shared<MapUDFLogicalOperator>(javaUDFDescriptor, 1);
    auto child = std::make_shared<SourceLogicalOperator>(
        std::make_shared<SchemaSourceDescriptor>(
            std::make_shared<Schema>()->addField("inputAttribute", DataTypeFactory::createUInt64())),
        2);
    mapUdfLogicalOperator->addChild(child);

    // After calling inferStringSignature, the map returned by `getHashBasesStringSignature` contains an entry.
    mapUdfLogicalOperator->inferStringSignature();
    auto hashBasedSignature = mapUdfLogicalOperator->getHashBasedSignature();
    ASSERT_TRUE(hashBasedSignature.size() == 1);

    // The signature ends with the string signature of the child.
    auto& signature = *hashBasedSignature.begin()->second.begin();
    auto& childSignature = *child->getHashBasedSignature().begin()->second.begin();
    NES_DEBUG("{}", signature);
    ASSERT_TRUE(signature.ends_with("." + childSignature));
}

// Regression test for https://github.com/nebulastream/nebulastream/issues/3484
// Setup: A MapJavaUDFLogicalOperator n1 has a parent p1 (e.g., a sink).
// We create a copies p2 of p1 and n2 of n1.
// The bug: When we try to add p2 as a parent to n2, the parent is not added because there already exists a parent with the same
// operator ID.
// Cause: n2 is a shallow copy of n1, so it retains the list of parents of n1 with the same IDs.
TEST_F(MapJavaUDFLogicalOperatorTest, AddParentToCopy) {
    // given: Create MapJavaUDFLogicalOperator with a parent.
    auto n1 = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder::createDefaultJavaUDFDescriptor());
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

// The following tests verify that the input schema of the UDF is compatible with the output schema of the parent operator.
// This test is the happy case, in which both schemas are compatible.
TEST_F(MapJavaUDFLogicalOperatorTest, InferSchemaCompatibleTypes) {
    auto udfInputSchema = Schema::create()
                              ->addField("charField", BasicType::INT8)
                              ->addField("shortField", BasicType::INT16)
                              ->addField("intField", BasicType::INT32)
                              ->addField("longField", BasicType::INT64)
                              ->addField("unsignedLongField", BasicType::INT64)
                              ->addField("floatField", BasicType::FLOAT32)
                              ->addField("doubleField", BasicType::FLOAT64)
                              ->addField("booleanField", BasicType::BOOLEAN)
                              ->addField("textField", BasicType::TEXT);
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder().setInputSchema(udfInputSchema).build());
    auto childOutputSchema = Schema::create()
                                 ->addField("source$charField", BasicType::INT8)
                                 ->addField("source$shortField", BasicType::INT16)
                                 ->addField("source$intField", BasicType::INT32)
                                 ->addField("source$longField", BasicType::INT64)
                                 ->addField("source$unsignedLongField", BasicType::UINT64)
                                 ->addField("source$floatField", BasicType::FLOAT32)
                                 ->addField("source$doubleField", BasicType::FLOAT64)
                                 ->addField("source$booleanField", BasicType::BOOLEAN)
                                 ->addField("source$textField", BasicType::TEXT);
    auto source = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(childOutputSchema)));
    op->addChild(source);
    // No exception is thrown here
    op->inferSchema();
}

// The schemas are not compatible because they contain the same field names but with different data types.
TEST_F(MapJavaUDFLogicalOperatorTest, InferSchemaInCompatibleTypes) {
    auto udfInputSchema = Schema::create()
                              ->addField("charField", BasicType::INT8)
                              ->addField("shortField", BasicType::INT16)
                              ->addField("intField", BasicType::INT32)
                              ->addField("longField", BasicType::INT64)
                              ->addField("floatField", BasicType::FLOAT32)
                              ->addField("doubleField", BasicType::FLOAT64)
                              ->addField("booleanField", BasicType::BOOLEAN)
                              ->addField("textField", BasicType::TEXT);
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder().setInputSchema(udfInputSchema).build());
    // Field types in the schema below are deliberately wrong.
    auto childOutputSchema = Schema::create()
                                 ->addField("source$charField", BasicType::INT16)
                                 ->addField("source$shortField", BasicType::INT32)
                                 ->addField("source$intField", BasicType::INT64)
                                 ->addField("source$longField", BasicType::INT8)
                                 ->addField("source$floatField", BasicType::FLOAT64)
                                 ->addField("source$doubleField", BasicType::FLOAT32)
                                 ->addField("source$booleanField", BasicType::TEXT)
                                 ->addField("source$textField", BasicType::BOOLEAN);
    auto source = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(childOutputSchema)));
    op->addChild(source);
    EXPECT_THROW(op->inferSchema(), NES::TypeInferenceException);
}

// The schemas are not compatible because they contain a different number of fields.
TEST_F(MapJavaUDFLogicalOperatorTest, InferSchemaDifferentSize) {
    auto udfInputSchema = Schema::create()->addField("field1", BasicType::INT8);
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder().setInputSchema(udfInputSchema).build());
    // Additional field in the schema below.
    auto childOutputSchema =
        Schema::create()->addField("source$field1", BasicType::INT8)->addField("source$field2", BasicType::INT8);
    auto source = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(childOutputSchema)));
    op->addChild(source);
    EXPECT_THROW(op->inferSchema(), NES::TypeInferenceException);
}

// The schemas are not compatible because a field is missing.
TEST_F(MapJavaUDFLogicalOperatorTest, InferSchemaMissingField) {
    auto udfInputSchema = Schema::create()->addField("field1", BasicType::INT8)->addField("missingField", BasicType::INT8);
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder().setInputSchema(udfInputSchema).build());
    // Different field name in the schema below.
    auto childOutputSchema = Schema::create()->addField("source$field1", BasicType::INT8);
    auto source = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(childOutputSchema)));
    op->addChild(source);
    EXPECT_THROW(op->inferSchema(), NES::TypeInferenceException);
}

// The schemas are not compatible because an unsigned integer field is used.
// The exception message should contain this information.
// The test does not verify this automatically but the debug log output can be manually checked.
TEST_F(MapJavaUDFLogicalOperatorTest, InferSchemaUnsignedIntegers) {
    auto udfInputSchema = Schema::create()
                              ->addField("charField", BasicType::INT8)
                              ->addField("shortField", BasicType::INT16)
                              ->addField("intField", BasicType::INT32);
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder().setInputSchema(udfInputSchema).build());
    // Field types in the schema below are unsigned.
    auto childOutputSchema = Schema::create()
                                 ->addField("source$charField", BasicType::UINT8)
                                 ->addField("source$shortField", BasicType::UINT16)
                                 ->addField("source$intField", BasicType::UINT32);
    auto source = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(childOutputSchema)));
    op->addChild(source);
    EXPECT_THROW(op->inferSchema(), NES::TypeInferenceException);
}

// The schemas are compatible because UINT64 in the child output schema are mapped to Java long in the UDF.
// The log should contain a WARN message indicating the possible integer overflow.
// The test does not verify this automatically but the log output can be manually checked.
TEST_F(MapJavaUDFLogicalOperatorTest, InferSchemaUnsignedLong) {
    auto udfInputSchema = Schema::create()->addField("unsignedLongField", BasicType::INT64);
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder().setInputSchema(udfInputSchema).build());
    auto childOutputSchema = Schema::create()->addField("source$unsignedLongField", BasicType::UINT64);
    auto source = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(childOutputSchema)));
    op->addChild(source);
    // No exception is thrown here
    op->inferSchema();
}

// The schemas are compatible if the Java UDF input type is not a complex object,
// the name of the field in the child operator output schema does not matter.
// Regression test for https://github.com/nebulastream/nebulastream/issues/4334
TEST_F(MapJavaUDFLogicalOperatorTest, InferSchemaSingleField) {
    auto udfInputSchema = Schema::create()->addField("", BasicType::INT64);
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder().setInputSchema(udfInputSchema).build());
    auto childOutputSchema = Schema::create()->addField("source$randomName", BasicType::INT64);
    auto source = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(childOutputSchema)));
    op->addChild(source);
    // No exception is thrown here
    op->inferSchema();
}

// The schemas are not compatible because the types are different.
// Regression test for https://github.com/nebulastream/nebulastream/issues/4334
TEST_F(MapJavaUDFLogicalOperatorTest, InferSchemaSingleFieldIncompatibleTypes) {
    auto udfInputSchema = Schema::create()->addField("", BasicType::INT64);
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder().setInputSchema(udfInputSchema).build());
    auto childOutputSchema = Schema::create()->addField("source$randomName", BasicType::INT32);
    auto source = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(childOutputSchema)));
    op->addChild(source);
    EXPECT_THROW(op->inferSchema(), NES::TypeInferenceException);
}

// The schemas are compatible because the UINT64 can be mapped to INT64.
// Regression test for https://github.com/nebulastream/nebulastream/issues/4334
TEST_F(MapJavaUDFLogicalOperatorTest, InferSchemaSingleFieldUnsignedLong) {
    auto udfInputSchema = Schema::create()->addField("", BasicType::INT64);
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(
        Catalogs::UDF::JavaUDFDescriptorBuilder().setInputSchema(udfInputSchema).build());
    auto childOutputSchema = Schema::create()->addField("source$randomName", BasicType::UINT64);
    auto source = LogicalOperatorFactory::createSourceOperator(SchemaSourceDescriptor::create(std::move(childOutputSchema)));
    op->addChild(source);
    // No exception is thrown here
    op->inferSchema();
}
}// namespace NES
