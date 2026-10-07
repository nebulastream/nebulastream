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

#include <Rules/Semantic/SemanticMapResolutionRule.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/InferModelNameLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SemanticFilterLogicalOperator.hpp>
#include <Operators/SemanticFilterNameLogicalOperator.hpp>
#include <Operators/SemanticMapLogicalOperator.hpp>
#include <Operators/SemanticMapNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/AsyncExecutionTrait.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <OptimizerTestUtils.hpp>
#include <QueryId.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class SemanticMapResolutionRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticMapResolutionRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
    std::shared_ptr<SemanticModelCatalog> catalog = std::make_shared<SemanticModelCatalog>();

    /// Registers a model reading `description` and producing `outputName`, both VARSIZED.
    void registerModel(const std::string& name, const std::string& outputName = "sentiment") const
    {
        SemanticModelConfig config;
        config.endpoint = "echo";
        config.modelName = "mock-model";
        config.backend = "mock";
        config.steps = {SemanticStep{
            .kind = SemanticStep::Kind::MAP,
            .prompt = "classify sentiment",
            .outputColumn = outputName,
            .outputValues = {},
            .defaultValue = ""}};
        catalog->registerModel(
            name,
            std::move(config),
            SemanticModelSchema{
                .inputs = SemanticFieldList{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
                .outputs = SemanticFieldList{UnqualifiedUnboundField{Identifier::parse(outputName), DataType::Type::VARSIZED}}});
    }

    /// Registers a filter model reading `description`: no OUTPUT, one FILTER step.
    void registerFilterModel(const std::string& name, const SemanticExecution execution = SemanticExecution::SYNCHRONOUS) const
    {
        SemanticModelConfig config;
        config.endpoint = "echo";
        config.modelName = "mock-model";
        config.backend = "mock";
        config.execution = execution;
        config.steps = {SemanticStep{
            .kind = SemanticStep::Kind::FILTER, .prompt = "is positive", .outputColumn = {}, .outputValues = {}, .defaultValue = {}}};
        catalog->registerModel(
            name,
            std::move(config),
            SemanticModelSchema{
                .inputs = SemanticFieldList{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}},
                .outputs = {}});
    }

    TypedLogicalOperator<SourceDescriptorLogicalOperator> descriptionSource(const std::string& name)
    {
        return utils.createSource(
            name,
            Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("description"), DataType::Type::VARSIZED}});
    }

    static LogicalPlan planWithRoot(LogicalOperator root)
    {
        return LogicalPlan{
            QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), {std::move(root)}};
    }
};

TEST_F(SemanticMapResolutionRuleTest, ResolvesNameOperatorAgainstCatalog)
{
    registerModel("sentimentModel");
    const auto source = descriptionSource("resolutionSource");
    const auto plan = planWithRoot(
        LogicalOperator{TypedLogicalOperator<SemanticMapNameLogicalOperator>{std::string{"sentimentModel"}, LogicalOperator{source}}});

    const auto resolved = SemanticMapResolutionRule{catalog}.apply(plan);

    const auto semanticMap = resolved.getRootOperators().at(0).tryGetAs<SemanticMapLogicalOperator>();
    ASSERT_TRUE(semanticMap.has_value());
    EXPECT_EQ(semanticMap->get().getModel().getName(), "sentimentModel");
    /// Resolution relinks without inferring; TypeInferenceRule supplies the schema afterwards.
    const auto inferred = resolved.getRootOperators().at(0).withInferredSchema();
    EXPECT_TRUE(inferred.getOutputSchema()[Identifier::parse("sentiment")].has_value());
    EXPECT_TRUE(inferred.getOutputSchema()[Identifier::parse("description")].has_value());
}

TEST_F(SemanticMapResolutionRuleTest, UnknownModelNameThrows)
{
    const auto source = descriptionSource("unknownModelSource");
    const auto plan = planWithRoot(
        LogicalOperator{TypedLogicalOperator<SemanticMapNameLogicalOperator>{std::string{"doesNotExist"}, LogicalOperator{source}}});

    ASSERT_EXCEPTION_ERRORCODE((void)SemanticMapResolutionRule{catalog}.apply(plan), NES::ErrorCode::UnknownSemanticModelName);
}

/// A plan without any SEM_MAP is a strict no-op: untouched subtrees are returned verbatim rather
/// than rebuilt.
TEST_F(SemanticMapResolutionRuleTest, LeavesPlansWithoutSemanticMapUntouched)
{
    const auto source = descriptionSource("noSemanticMapSource");
    const auto plan = planWithRoot(LogicalOperator{source});

    const auto resolved = SemanticMapResolutionRule{catalog}.apply(plan);

    ASSERT_EQ(resolved.getRootOperators().size(), 1);
    EXPECT_EQ(resolved.getRootOperators().at(0), plan.getRootOperators().at(0));
}

/// An unresolved InferModelName subtree must not be touched: its schema accessors are
/// PRECONDITION-guarded, so a regression here is process death rather than a test failure.
TEST_F(SemanticMapResolutionRuleTest, DoesNotTouchUnresolvedInferModelNameSubtree)
{
    const auto source = descriptionSource("inferModelSource");
    const auto plan = planWithRoot(
        LogicalOperator{TypedLogicalOperator<InferModelNameLogicalOperator>{std::string{"someMlModel"}, LogicalOperator{source}}});

    const auto resolved = SemanticMapResolutionRule{catalog}.apply(plan);

    ASSERT_EQ(resolved.getRootOperators().size(), 1);
    EXPECT_EQ(resolved.getRootOperators().at(0), plan.getRootOperators().at(0));
}

/// SEM_MAP(m, MODEL_INFERENCE(...)): this rule runs before InferModelResolutionRule, so the
/// SEM_MAP's child is still the guarded placeholder. Inferring the child's schema eagerly while
/// resolving — what the constructor with a child does — would kill the process.
TEST_F(SemanticMapResolutionRuleTest, ResolvesDirectlyAboveUnresolvedInferModelName)
{
    registerModel("sentimentModel");
    const auto source = descriptionSource("stackedSource");
    const auto inferModelName
        = LogicalOperator{TypedLogicalOperator<InferModelNameLogicalOperator>{std::string{"someMlModel"}, LogicalOperator{source}}};
    const auto plan = planWithRoot(
        LogicalOperator{TypedLogicalOperator<SemanticMapNameLogicalOperator>{std::string{"sentimentModel"}, inferModelName}});

    const auto resolved = SemanticMapResolutionRule{catalog}.apply(plan);

    const auto root = resolved.getRootOperators().at(0);
    ASSERT_TRUE(root.tryGetAs<SemanticMapLogicalOperator>().has_value());
    ASSERT_EQ(root.getChildren().size(), 1);
    EXPECT_EQ(root.getChildren().at(0), inferModelName);
}

/// Parents of a resolved SEM_MAP are relinked to the resolved operator, all the way to the root.
TEST_F(SemanticMapResolutionRuleTest, RelinksTheSpineAboveNestedSemanticMaps)
{
    registerModel("sentimentModel", "sentiment");
    registerModel("summaryModel", "summary");
    const auto source = descriptionSource("nestedSource");
    const auto inner
        = LogicalOperator{TypedLogicalOperator<SemanticMapNameLogicalOperator>{std::string{"sentimentModel"}, LogicalOperator{source}}};
    const auto outer = LogicalOperator{TypedLogicalOperator<SemanticMapNameLogicalOperator>{std::string{"summaryModel"}, inner}};

    const auto resolved = SemanticMapResolutionRule{catalog}.apply(planWithRoot(outer));

    const auto root = resolved.getRootOperators().at(0);
    ASSERT_TRUE(root.tryGetAs<SemanticMapLogicalOperator>().has_value());
    ASSERT_TRUE(root.getChildren().at(0).tryGetAs<SemanticMapLogicalOperator>().has_value());

    const auto inferred = root.withInferredSchema();
    EXPECT_TRUE(inferred.getOutputSchema()[Identifier::parse("sentiment")].has_value());
    EXPECT_TRUE(inferred.getOutputSchema()[Identifier::parse("summary")].has_value());
}

TEST_F(SemanticMapResolutionRuleTest, ResolvesFilterNameOperator)
{
    registerFilterModel("positive");
    const auto source = descriptionSource("filterSource");
    const auto plan = planWithRoot(
        LogicalOperator{TypedLogicalOperator<SemanticFilterNameLogicalOperator>{std::string{"positive"}, LogicalOperator{source}}});

    const auto resolved = SemanticMapResolutionRule{catalog}.apply(plan);

    const auto filter = resolved.getRootOperators().at(0).tryGetAs<SemanticFilterLogicalOperator>();
    ASSERT_TRUE(filter.has_value());
    EXPECT_EQ(filter->get().getModel().getName(), "positive");
    EXPECT_FALSE(resolved.getRootOperators().at(0).getTraitSet().tryGet<AsyncExecutionTrait>().has_value());
    const auto inferred = resolved.getRootOperators().at(0).withInferredSchema();
    EXPECT_EQ(inferred.getOutputSchema().size(), 1);
}

TEST_F(SemanticMapResolutionRuleTest, AsyncFilterGetsTheFilterExecutor)
{
    registerFilterModel("positive", SemanticExecution::ASYNCHRONOUS);
    const auto source = descriptionSource("asyncFilterSource");
    const auto plan = planWithRoot(
        LogicalOperator{TypedLogicalOperator<SemanticFilterNameLogicalOperator>{std::string{"positive"}, LogicalOperator{source}}});

    const auto resolved = SemanticMapResolutionRule{catalog}.apply(plan);

    const auto trait = resolved.getRootOperators().at(0).getTraitSet().tryGet<AsyncExecutionTrait>();
    ASSERT_TRUE(trait.has_value());
    EXPECT_EQ(trait.value()->executorType, "SemanticFilter");
}

/// A model is one kind or the other; using it with the wrong operator is the query's mistake.
TEST_F(SemanticMapResolutionRuleTest, WrongModelKindThrows)
{
    registerModel("sentimentModel");
    registerFilterModel("positive");
    const auto source = descriptionSource("wrongKindSource");

    const auto mapModelInFilter = planWithRoot(
        LogicalOperator{TypedLogicalOperator<SemanticFilterNameLogicalOperator>{std::string{"sentimentModel"}, LogicalOperator{source}}});
    ASSERT_EXCEPTION_ERRORCODE((void)SemanticMapResolutionRule{catalog}.apply(mapModelInFilter), NES::ErrorCode::InvalidSemanticModel);

    const auto filterModelInMap = planWithRoot(
        LogicalOperator{TypedLogicalOperator<SemanticMapNameLogicalOperator>{std::string{"positive"}, LogicalOperator{source}}});
    ASSERT_EXCEPTION_ERRORCODE((void)SemanticMapResolutionRule{catalog}.apply(filterModelInMap), NES::ErrorCode::InvalidSemanticModel);
}

TEST_F(SemanticMapResolutionRuleTest, ResolvesFilterAndMapNestedInEachOther)
{
    registerModel("sentimentModel");
    registerFilterModel("positive");
    const auto source = descriptionSource("mixedSource");
    const auto inner
        = LogicalOperator{TypedLogicalOperator<SemanticMapNameLogicalOperator>{std::string{"sentimentModel"}, LogicalOperator{source}}};
    const auto outer = LogicalOperator{TypedLogicalOperator<SemanticFilterNameLogicalOperator>{std::string{"positive"}, inner}};

    const auto root = SemanticMapResolutionRule{catalog}.apply(planWithRoot(outer)).getRootOperators().at(0);

    ASSERT_TRUE(root.tryGetAs<SemanticFilterLogicalOperator>().has_value());
    ASSERT_TRUE(root.getChildren().at(0).tryGetAs<SemanticMapLogicalOperator>().has_value());
    EXPECT_TRUE(root.withInferredSchema().getOutputSchema()[Identifier::parse("sentiment")].has_value());
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
