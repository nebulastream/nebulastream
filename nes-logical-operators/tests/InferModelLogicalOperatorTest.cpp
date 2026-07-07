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

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/InferModelNameLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>

namespace NES
{

namespace
{

/// Register a fresh model in a shared test-local catalog and return the RegisteredModel.
/// The catalog is a static local so entries accumulate across tests; names are made unique
/// by a monotonic counter so re-registering is never needed.
RegisteredModel loadModel(std::string_view onnxFile, ModelFieldList inputs, ModelFieldList outputs)
{
    static ModelCatalog catalog;
    static std::atomic<size_t> counter{0};
    const auto name = fmt::format("m_{}", counter.fetch_add(1));

    catalog.registerModel(
        name,
        std::filesystem::path(INFERENCE_TEST_DATA) / std::string(onnxFile),
        ModelSchema{.inputs = std::move(inputs), .outputs = std::move(outputs)});
    return catalog.load(name);
}

/// Default 1-input/1-output FLOAT32 model: takes `in_0`, produces `prediction`.
RegisteredModel defaultModel()
{
    return loadModel(
        "tiny_1_to_1.onnx",
        ModelFieldList{UnqualifiedUnboundField{Identifier::parse("in_0"), DataType::Type::FLOAT32}},
        ModelFieldList{UnqualifiedUnboundField{Identifier::parse("prediction"), DataType::Type::FLOAT32}});
}

/// NOLINTBEGIN(readability-magic-numbers, bugprone-unchecked-optional-access)
/// Build a SourceDescriptorLogicalOperator with the given field schema; used as the InferModel
/// child in schema-inference tests so the resulting Field-typed schema is fully bound to a
/// real producing operator.
TypedLogicalOperator<SourceDescriptorLogicalOperator>
makeSourceWithSchema(SourceCatalog& catalog, std::string_view sourceName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    const auto logical = catalog.addLogicalSource(Identifier::parse(std::string{sourceName}), schema).value();
    const Schema<LiteralConfigValue, Ordered> sourceConfig{{QualifiedIdentifier::create(Identifier::parse("file_path")), "/dev/null"}};
    const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("type"), "CSV"}};
    const auto descriptor
        = catalog.addPhysicalSource(logical, Identifier::parse("file"), Host("localhost"), sourceConfig, parserConfig).value();
    return SourceDescriptorLogicalOperator::create(descriptor);
}

}

class InferModelLogicalOperatorTest : public ::testing::Test
{
};

/// Construction and basic accessors on InferModelLogicalOperator (no child set).
TEST_F(InferModelLogicalOperatorTest, BasicProperties)
{
    const auto op = TypedLogicalOperator<InferModelLogicalOperator>{defaultModel()};

    EXPECT_NO_THROW({
        const LogicalOperator wrapped{op};
        (void)wrapped;
    });
    EXPECT_EQ(op->getName(), "InferModel");
    EXPECT_TRUE(op->getChildren().empty());

    const auto description = op->explain(ExplainVerbosity::Short, OperatorId{1});
    EXPECT_FALSE(description.empty());
    EXPECT_NE(description.find("INFER_MODEL"), std::string::npos);

    const auto debugDesc = op->explain(ExplainVerbosity::Debug, OperatorId{42});
    EXPECT_NE(debugDesc.find("42"), std::string::npos);

    EXPECT_EQ(op->getModel().getSchema().inputs.size(), 1U);
    EXPECT_EQ(op->getModel().getSchema().outputs.size(), 1U);
}

/// Happy path: model input field is present in the child's schema; output is child fields ∪ model outputs.
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceHappyPath)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("in_0"), DataType::Type::FLOAT32},
            UnqualifiedUnboundField{Identifier::parse("sibling"), DataType::Type::UINT64}});

    const auto op = TypedLogicalOperator<InferModelLogicalOperator>{defaultModel(), LogicalOperator{source}};

    /// Output = child fields (in_0, sibling) ∪ model outputs (prediction) — 3 fields total.
    const auto outputSchema = op->getOutputSchema();
    EXPECT_EQ(outputSchema.size(), 3U);
    EXPECT_TRUE(outputSchema[Identifier::parse("in_0")].has_value());
    EXPECT_TRUE(outputSchema[Identifier::parse("sibling")].has_value());
    EXPECT_TRUE(outputSchema[Identifier::parse("prediction")].has_value());

    const auto predictionField = outputSchema[Identifier::parse("prediction")];
    ASSERT_TRUE(predictionField.has_value());
    EXPECT_EQ(predictionField->getDataType().type, DataType::Type::FLOAT32);
    EXPECT_FALSE(predictionField->getDataType().nullable);
}

/// Model input field missing in child's schema throws CannotInferSchema.
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceMissingField)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("other"), DataType::Type::FLOAT32}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<InferModelLogicalOperator>{defaultModel(), LogicalOperator{source}}), NES::ErrorCode::CannotInferSchema);
}

/// Model input field present but with the wrong type throws CannotInferSchema.
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceTypeMismatch)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("in_0"), DataType::Type::INT32}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<InferModelLogicalOperator>{defaultModel(), LogicalOperator{source}}), NES::ErrorCode::CannotInferSchema);
}

/// Nullable model input field is rejected; model inputs must be non-nullable.
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceRejectsNullableInput)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("in_0"), DataType{DataType::Type::FLOAT32, DataType::NULLABLE::IS_NULLABLE}}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<InferModelLogicalOperator>{defaultModel(), LogicalOperator{source}}), NES::ErrorCode::CannotInferSchema);
}

/// A model output that shadows an upstream field name is a collision; surfaced as CannotInferSchema
/// (consistent with the join operator's behavior — output schema is the strict union).
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceRejectsNameCollision)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("in_0"), DataType::Type::FLOAT32},
            UnqualifiedUnboundField{Identifier::parse("prediction"), DataType::Type::FLOAT32}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<InferModelLogicalOperator>{defaultModel(), LogicalOperator{source}}), NES::ErrorCode::CannotInferSchema);
}

/// VARSIZED model input passes through fine when the child supplies a matching VARSIZED field.
TEST_F(InferModelLogicalOperatorTest, SchemaInferenceVarsizedInputAccepted)
{
    const auto model = loadModel(
        "tiny_1_to_1.onnx",
        ModelFieldList{UnqualifiedUnboundField{Identifier::parse("text"), DataType::Type::VARSIZED}},
        ModelFieldList{UnqualifiedUnboundField{Identifier::parse("embedding"), DataType::Type::FLOAT32}});

    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("text"), DataType::Type::VARSIZED}});

    const auto op = TypedLogicalOperator<InferModelLogicalOperator>{model, LogicalOperator{source}};
    const auto outputSchema = op->getOutputSchema();
    EXPECT_EQ(outputSchema.size(), 2U);
    EXPECT_TRUE(outputSchema[Identifier::parse("text")].has_value());
    EXPECT_TRUE(outputSchema[Identifier::parse("embedding")].has_value());
}

/// InferModelNameLogicalOperator construction, getName, explain.
TEST_F(InferModelLogicalOperatorTest, NameVariantBasicProperties)
{
    const auto nameOp = TypedLogicalOperator<InferModelNameLogicalOperator>{std::string{"myModel"}};

    EXPECT_EQ(std::string{nameOp->getName()}, "InferModelName");
    EXPECT_EQ(nameOp->getModelName(), "myModel");
    EXPECT_TRUE(nameOp->getChildren().empty());

    const auto desc = nameOp->explain(ExplainVerbosity::Short, OperatorId{2});
    EXPECT_FALSE(desc.empty());
    EXPECT_NE(desc.find("myModel"), std::string::npos);

    EXPECT_NO_THROW({
        const LogicalOperator wrapped{nameOp};
        (void)wrapped;
    });
}

/// NOLINTEND(readability-magic-numbers, bugprone-unchecked-optional-access)

}
