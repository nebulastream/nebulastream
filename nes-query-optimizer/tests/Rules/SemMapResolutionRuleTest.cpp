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

#include <Rules/Semantic/SemMapResolutionRule.hpp>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/InferModelNameLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SemMapLogicalOperator.hpp>
#include <Operators/SemMapNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryId.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <OptimizerTestUtils.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticModelConfig.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class SemMapResolutionRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemMapResolutionRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
};

TEST_F(SemMapResolutionRuleTest, ResolvesNameOperatorAgainstCatalog)
{
    auto semanticModelCatalog = std::make_shared<SemanticModelCatalog>();
    semanticModelCatalog->registerModel(
        "sentimentModel",
        SemanticModelConfig{
            .baseUrl = "mock://localhost",
            .model = "mock-model",
            .apiKeyEnv = std::nullopt,
            .steps = {SemanticStep{
                .kind = SemanticStep::Kind::MAP, .prompt = "classify sentiment", .outputValues = {}, .defaultValue = ""}}},
        SemanticModelSchema{
            .inputs = SemanticModelFieldList{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
            .outputs = SemanticModelFieldList{UnqualifiedUnboundField{Identifier::parse("sentiment"), DataType::Type::VARSIZED}}});

    const auto source = utils.createSource(
        "resolutionSource",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}});

    const std::vector<UnqualifiedUnboundField> callSiteInputs{
        UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}};
    const auto semMapName
        = TypedLogicalOperator<SemMapNameLogicalOperator>{std::string{"sentimentModel"}, callSiteInputs, LogicalOperator{source}};

    const auto plan = LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}),
        {LogicalOperator{semMapName}}};

    const auto resolved = SemMapResolutionRule{semanticModelCatalog}.apply(plan);

    const auto semMap = resolved.getRootOperators().at(0);
    const auto semMapOp = semMap.tryGetAs<SemMapLogicalOperator>();
    ASSERT_TRUE(semMapOp.has_value());
    EXPECT_EQ(semMapOp->get().getModel().getName(), "sentimentModel");
    EXPECT_EQ(semMapOp->get().getCallSiteInputs(), callSiteInputs);
    EXPECT_TRUE(semMapOp->get().getOutputSchema()[Identifier::parse("sentiment")].has_value());
}

TEST_F(SemMapResolutionRuleTest, CarriesOutputAliasThroughResolution)
{
    auto semanticModelCatalog = std::make_shared<SemanticModelCatalog>();
    semanticModelCatalog->registerModel(
        "sentimentModel",
        SemanticModelConfig{
            .baseUrl = "mock://localhost",
            .model = "mock-model",
            .apiKeyEnv = std::nullopt,
            .steps = {SemanticStep{
                .kind = SemanticStep::Kind::MAP, .prompt = "classify sentiment", .outputValues = {}, .defaultValue = ""}}},
        SemanticModelSchema{
            .inputs = SemanticModelFieldList{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
            .outputs = SemanticModelFieldList{UnqualifiedUnboundField{Identifier::parse("sentiment"), DataType::Type::VARSIZED}}});

    const auto source = utils.createSource(
        "aliasResolutionSource",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}});

    const std::vector<UnqualifiedUnboundField> callSiteInputs{
        UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}};
    const auto semMapName = TypedLogicalOperator<SemMapNameLogicalOperator>{
        std::string{"sentimentModel"}, callSiteInputs, LogicalOperator{source}, Identifier::parse("cls")};

    const auto plan = LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}),
        {LogicalOperator{semMapName}}};

    const auto resolved = SemMapResolutionRule{semanticModelCatalog}.apply(plan);

    const auto semMap = resolved.getRootOperators().at(0);
    const auto semMapOp = semMap.tryGetAs<SemMapLogicalOperator>();
    ASSERT_TRUE(semMapOp.has_value());
    ASSERT_TRUE(semMapOp->get().getOutputAlias().has_value());
    EXPECT_EQ(*semMapOp->get().getOutputAlias(), Identifier::parse("cls"));
    /// The alias renames the model's declared OUTPUT field in the inferred schema.
    EXPECT_TRUE(semMapOp->get().getOutputSchema()[Identifier::parse("cls")].has_value());
    EXPECT_FALSE(semMapOp->get().getOutputSchema()[Identifier::parse("sentiment")].has_value());
}

TEST_F(SemMapResolutionRuleTest, UnknownModelNameThrows)
{
    auto semanticModelCatalog = std::make_shared<SemanticModelCatalog>();

    const auto source = utils.createSource(
        "unknownModelSource",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}});

    const std::vector<UnqualifiedUnboundField> callSiteInputs{
        UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}};
    const auto semMapName
        = TypedLogicalOperator<SemMapNameLogicalOperator>{std::string{"doesNotExist"}, callSiteInputs, LogicalOperator{source}};

    const auto plan = LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}),
        {LogicalOperator{semMapName}}};

    ASSERT_EXCEPTION_ERRORCODE((void)SemMapResolutionRule{semanticModelCatalog}.apply(plan), NES::ErrorCode::UnknownModelName);
}

/// A plan without any SemMapName is a strict no-op: the spine-only traversal returns untouched
/// subtrees verbatim rather than rebuilding via withChildren (plan §M4 as-built note, "resolution
/// rule traversal is spine-only").
TEST_F(SemMapResolutionRuleTest, LeavesPlansWithoutSemMapUntouched)
{
    auto semanticModelCatalog = std::make_shared<SemanticModelCatalog>();

    const auto source = utils.createSource(
        "noSemMapSource",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}});

    const auto plan = LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), {LogicalOperator{source}}};

    const auto resolved = SemMapResolutionRule{semanticModelCatalog}.apply(plan);

    ASSERT_EQ(resolved.getRootOperators().size(), 1);
    EXPECT_EQ(resolved.getRootOperators().at(0), plan.getRootOperators().at(0));
}

/// A subtree holding an unresolved InferModelNameLogicalOperator must never be touched by this
/// rule: InferModelNameLogicalOperator::withInferredSchema() PRECONDITION-aborts the whole process
/// (it requires model resolution first), and the old rebuild-everything fallback used to call
/// exactly that on every subtree it walked over, including parents of unresolved InferModelName
/// operators (plan §M4 as-built note). This is the case that maps directly to the systest crash
/// the spine-only rewrite fixed: it fails loudly (process death) rather than a clean test failure
/// if the fallback ever regresses to op.withChildren(children) unconditionally.
TEST_F(SemMapResolutionRuleTest, DoesNotTouchUnresolvedInferModelNameSubtree)
{
    auto semanticModelCatalog = std::make_shared<SemanticModelCatalog>();

    const auto source = utils.createSource(
        "inferModelSource",
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}});

    const auto inferModelName = TypedLogicalOperator<InferModelNameLogicalOperator>{std::string{"someMlModel"}, LogicalOperator{source}};

    const auto plan = LogicalPlan{
        QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}),
        {LogicalOperator{inferModelName}}};

    const auto resolved = SemMapResolutionRule{semanticModelCatalog}.apply(plan);

    ASSERT_EQ(resolved.getRootOperators().size(), 1);
    const auto resolvedInferModelName = resolved.getRootOperators().at(0).tryGetAs<InferModelNameLogicalOperator>();
    ASSERT_TRUE(resolvedInferModelName.has_value());
    EXPECT_EQ(resolvedInferModelName->get().getModelName(), "someMlModel");
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
