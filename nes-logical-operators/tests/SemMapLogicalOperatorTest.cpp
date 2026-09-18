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
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SemMapLogicalOperator.hpp>
#include <Operators/SemMapNameLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticModelConfig.hpp>

namespace NES
{

namespace
{

/// Register a fresh semantic model in a shared test-local catalog and return the RegisteredSemanticModel.
/// The catalog is a static local so entries accumulate across tests; names are made unique by a
/// monotonic counter so re-registering is never needed. Uses a mock:// base URL so registration
/// validation (URL parses, MODEL/PROMPT non-empty) passes without contacting a real endpoint.
RegisteredSemanticModel loadModel(SemanticModelFieldList inputs, SemanticModelFieldList outputs)
{
    static SemanticModelCatalog catalog;
    static std::atomic<size_t> counter{0};
    const auto name = fmt::format("sm_{}", counter.fetch_add(1));

    catalog.registerModel(
        name,
        SemanticModelConfig{
            .baseUrl = "mock://localhost",
            .model = "mock-model",
            .prompt = "classify this",
            .apiKeyEnv = std::nullopt,
            .outputValues = std::nullopt},
        SemanticModelSchema{.inputs = std::move(inputs), .outputs = std::move(outputs)});
    return catalog.load(name);
}

/// Default 1-input/1-output VARSIZED model: takes `description`, produces `sentiment`.
RegisteredSemanticModel defaultModel()
{
    return loadModel(
        SemanticModelFieldList{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
        SemanticModelFieldList{UnqualifiedUnboundField{Identifier::parse("sentiment"), DataType::Type::VARSIZED}});
}

/// NOLINTBEGIN(readability-magic-numbers, bugprone-unchecked-optional-access)
/// Build a SourceDescriptorLogicalOperator with the given field schema; used as the SemMap child
/// in schema-inference tests so the resulting Field-typed schema is fully bound to a real
/// producing operator.
TypedLogicalOperator<SourceDescriptorLogicalOperator>
makeSourceWithSchema(SourceCatalog& catalog, std::string_view sourceName, const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    const auto logical = catalog.addLogicalSource(Identifier::parse(std::string{sourceName}), schema).value();
    const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("file_path"), "/dev/null"}};
    const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("type"), "CSV"}};
    const auto descriptor
        = catalog.addPhysicalSource(logical, Identifier::parse("file"), Host("localhost"), sourceConfig, parserConfig).value();
    return SourceDescriptorLogicalOperator::create(descriptor);
}

}

class SemMapLogicalOperatorTest : public ::testing::Test
{
};

/// Construction and basic accessors on SemMapLogicalOperator (no child set).
TEST_F(SemMapLogicalOperatorTest, BasicProperties)
{
    const auto op = TypedLogicalOperator<SemMapLogicalOperator>{
        defaultModel(), std::vector<UnqualifiedUnboundField>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}}};

    EXPECT_NO_THROW({
        const LogicalOperator wrapped{op};
        (void)wrapped;
    });
    EXPECT_EQ(op->getName(), "SemMap");
    EXPECT_TRUE(op->getChildren().empty());

    const auto description = op->explain(ExplainVerbosity::Short, OperatorId{1});
    EXPECT_FALSE(description.empty());
    EXPECT_NE(description.find("SEM_MAP"), std::string::npos);

    const auto debugDesc = op->explain(ExplainVerbosity::Debug, OperatorId{42});
    EXPECT_NE(debugDesc.find("42"), std::string::npos);

    EXPECT_EQ(op->getModel().getSchema().inputs.size(), 1U);
    EXPECT_EQ(op->getModel().getSchema().outputs.size(), 1U);
}

/// Happy path: call-site input field is present in the child's schema; output is child fields ∪ model outputs.
TEST_F(SemMapLogicalOperatorTest, SchemaInferenceHappyPath)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("sibling"), DataType::Type::UINT64}});

    const auto op = TypedLogicalOperator<SemMapLogicalOperator>{
        defaultModel(),
        std::vector<UnqualifiedUnboundField>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
        LogicalOperator{source}};

    /// Output = child fields (description, sibling) ∪ model outputs (sentiment) — 3 fields total.
    const auto outputSchema = op->getOutputSchema();
    EXPECT_EQ(outputSchema.size(), 3U);
    EXPECT_TRUE(outputSchema[Identifier::parse("description")].has_value());
    EXPECT_TRUE(outputSchema[Identifier::parse("sibling")].has_value());
    EXPECT_TRUE(outputSchema[Identifier::parse("sentiment")].has_value());

    const auto sentimentField = outputSchema[Identifier::parse("sentiment")];
    ASSERT_TRUE(sentimentField.has_value());
    EXPECT_EQ(sentimentField->getDataType().type, DataType::Type::VARSIZED);
    EXPECT_FALSE(sentimentField->getDataType().nullable);
}

/// Call-site input arity differs from the catalog's declared INPUT arity throws CannotInferSchema.
TEST_F(SemMapLogicalOperatorTest, CallSiteArityMismatch)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("other"), DataType::Type::VARSIZED}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<SemMapLogicalOperator>{
            defaultModel(),
            std::vector<UnqualifiedUnboundField>{
                UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED},
                UnqualifiedUnboundField{Identifier::parse("other"), DataType::Type::VARSIZED}},
            LogicalOperator{source}}),
        NES::ErrorCode::CannotInferSchema);
}

/// Call-site input field missing in child's schema throws CannotInferSchema.
TEST_F(SemMapLogicalOperatorTest, CallSiteFieldMissing)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("other"), DataType::Type::VARSIZED}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<SemMapLogicalOperator>{
            defaultModel(),
            std::vector<UnqualifiedUnboundField>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
            LogicalOperator{source}}),
        NES::ErrorCode::CannotInferSchema);
}

/// Nullable call-site input field is rejected; SemMap inputs must be non-nullable.
TEST_F(SemMapLogicalOperatorTest, SchemaInferenceRejectsNullableInput)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("description"), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE}}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<SemMapLogicalOperator>{
            defaultModel(),
            std::vector<UnqualifiedUnboundField>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
            LogicalOperator{source}}),
        NES::ErrorCode::CannotInferSchema);
}

/// A non-VARSIZED call-site input field is rejected — SEM_MAP requires VARSIZED (looser than
/// InferModel's exact-type match, but still enforced).
TEST_F(SemMapLogicalOperatorTest, SchemaInferenceRejectsNonVarsizedInput)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog, "src", Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::UINT64}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<SemMapLogicalOperator>{
            defaultModel(),
            std::vector<UnqualifiedUnboundField>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
            LogicalOperator{source}}),
        NES::ErrorCode::CannotInferSchema);
}

/// A model output that shadows an upstream field name is a collision; surfaced as CannotInferSchema.
TEST_F(SemMapLogicalOperatorTest, SchemaInferenceRejectsNameCollision)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("sentiment"), DataType::Type::VARSIZED}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<SemMapLogicalOperator>{
            defaultModel(),
            std::vector<UnqualifiedUnboundField>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
            LogicalOperator{source}}),
        NES::ErrorCode::CannotInferSchema);
}

/// An output alias renames the model's single OUTPUT field, keeping its declared type; the
/// model's original output name disappears from the schema.
TEST_F(SemMapLogicalOperatorTest, AliasRenamesSingleOutput)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("sibling"), DataType::Type::UINT64}});

    const auto op = TypedLogicalOperator<SemMapLogicalOperator>{
        defaultModel(),
        std::vector<UnqualifiedUnboundField>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
        LogicalOperator{source},
        Identifier::parse("cls")};

    /// Output = child fields (description, sibling) ∪ renamed model output (cls) — 3 fields total.
    const auto outputSchema = op->getOutputSchema();
    EXPECT_EQ(outputSchema.size(), 3U);
    EXPECT_TRUE(outputSchema[Identifier::parse("cls")].has_value());
    EXPECT_FALSE(outputSchema[Identifier::parse("sentiment")].has_value());

    const auto clsField = outputSchema[Identifier::parse("cls")];
    ASSERT_TRUE(clsField.has_value());
    EXPECT_EQ(clsField->getDataType().type, DataType::Type::VARSIZED);
    EXPECT_FALSE(clsField->getDataType().nullable);
}

/// An output alias requires the model to declare exactly one OUTPUT field.
TEST_F(SemMapLogicalOperatorTest, AliasRequiresSingleOutputModel)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}});

    const auto multiOutputModel = loadModel(
        SemanticModelFieldList{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
        SemanticModelFieldList{
            UnqualifiedUnboundField{Identifier::parse("firstOutput"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("secondOutput"), DataType::Type::VARSIZED}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<SemMapLogicalOperator>{
            multiOutputModel,
            std::vector<UnqualifiedUnboundField>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
            LogicalOperator{source},
            Identifier::parse("cls")}),
        NES::ErrorCode::CannotInferSchema);
}

/// An output alias that shadows an upstream field name is a collision; surfaced as CannotInferSchema.
TEST_F(SemMapLogicalOperatorTest, AliasCollidesWithChildField)
{
    SourceCatalog catalog;
    auto source = makeSourceWithSchema(
        catalog,
        "src",
        Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("label"), DataType::Type::VARSIZED}});

    ASSERT_EXCEPTION_ERRORCODE(
        (TypedLogicalOperator<SemMapLogicalOperator>{
            defaultModel(),
            std::vector<UnqualifiedUnboundField>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
            LogicalOperator{source},
            Identifier::parse("label")}),
        NES::ErrorCode::CannotInferSchema);
}

/// SemMapNameLogicalOperator construction, getName, explain. withInferredSchema/getOutputSchema
/// abort by design (require model resolution first) and are deliberately never exercised here.
TEST_F(SemMapLogicalOperatorTest, NameVariantBasicProperties)
{
    const std::vector<UnqualifiedUnboundField> callSiteInputs{
        UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}};
    const auto nameOp = TypedLogicalOperator<SemMapNameLogicalOperator>{std::string{"myModel"}, callSiteInputs};

    EXPECT_EQ(std::string{nameOp->getName()}, "SemMapName");
    EXPECT_EQ(nameOp->getModelName(), "myModel");
    EXPECT_EQ(nameOp->getCallSiteInputs(), callSiteInputs);
    EXPECT_TRUE(nameOp->getChildren().empty());

    const auto desc = nameOp->explain(ExplainVerbosity::Short, OperatorId{2});
    EXPECT_FALSE(desc.empty());
    EXPECT_NE(desc.find("myModel"), std::string::npos);
    /// Identifiers are normalized uppercase by Identifier::parse.
    EXPECT_NE(desc.find("DESCRIPTION"), std::string::npos);

    EXPECT_NO_THROW({
        const LogicalOperator wrapped{nameOp};
        (void)wrapped;
    });
}

/// The name variant carries the optional output alias through construction, getter, explain,
/// equality and hash.
TEST_F(SemMapLogicalOperatorTest, NameVariantCarriesOutputAlias)
{
    const std::vector<UnqualifiedUnboundField> callSiteInputs{
        UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}};
    const auto withAlias
        = TypedLogicalOperator<SemMapNameLogicalOperator>{std::string{"myModel"}, callSiteInputs, Identifier::parse("cls")};
    const auto withAliasAgain
        = TypedLogicalOperator<SemMapNameLogicalOperator>{std::string{"myModel"}, callSiteInputs, Identifier::parse("cls")};
    const auto withoutAlias = TypedLogicalOperator<SemMapNameLogicalOperator>{std::string{"myModel"}, callSiteInputs};

    ASSERT_TRUE(withAlias->getOutputAlias().has_value());
    /// Identifiers are normalized uppercase by Identifier::parse.
    EXPECT_EQ(*withAlias->getOutputAlias(), Identifier::parse("cls"));
    EXPECT_FALSE(withoutAlias->getOutputAlias().has_value());

    const auto desc = withAlias->explain(ExplainVerbosity::Short, OperatorId{2});
    EXPECT_NE(desc.find("outputAlias: CLS"), std::string::npos);
    EXPECT_EQ(withoutAlias->explain(ExplainVerbosity::Short, OperatorId{2}).find("outputAlias"), std::string::npos);

    EXPECT_TRUE(*withAlias == *withAliasAgain);
    EXPECT_FALSE(*withAlias == *withoutAlias);
    const std::hash<NES::SemMapNameLogicalOperator> hasher;
    EXPECT_EQ(hasher(*withAlias), hasher(*withAliasAgain));
}

/// NOLINTEND(readability-magic-numbers, bugprone-unchecked-optional-access)

}
