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

#include <Rules/Semantic/SemanticFusionRule.hpp>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SemanticFilterLogicalOperator.hpp>
#include <Operators/SemanticMapLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/AsyncExecutionTrait.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <OptimizerTestUtils.hpp>
#include <QueryId.hpp>
#include <SemanticAsyncWiring.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class SemanticFusionRuleTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticFusionRuleTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;
    SemanticModelCatalog catalog;

    static UnqualifiedUnboundField field(const std::string& name)
    {
        return UnqualifiedUnboundField{Identifier::parse(name), DataType::Type::VARSIZED};
    }

    /// A fusable model over (description, title). `output` empty makes it a filter model.
    RegisteredSemanticModel
    model(const std::string& name, const std::string& output = {}, const std::function<void(SemanticModelConfig&)>& tweak = {})
    {
        SemanticModelConfig config;
        config.endpoint = "echo";
        config.modelName = "mock-model";
        config.backend = "mock";
        config.fusion = true;
        config.steps = {SemanticStep{
            .kind = output.empty() ? SemanticStep::Kind::FILTER : SemanticStep::Kind::MAP,
            .prompt = "prompt of " + name,
            .outputColumn = output,
            .outputValues = {},
            .defaultValue = {}}};
        SemanticModelSchema schema{.inputs = SemanticFieldList{field("description"), field("title")}, .outputs = {}};
        if (!output.empty())
        {
            schema.outputs = SemanticFieldList{field(output)};
        }
        if (tweak)
        {
            tweak(config);
        }
        if (config.execution == SemanticExecution::ASYNCHRONOUS)
        {
            config.batchSize = 4;
        }
        catalog.registerModel(name, std::move(config), std::move(schema));
        return catalog.load(name);
    }

    /// The operator the resolution rule would have produced, async marker included.
    static LogicalOperator resolved(const RegisteredSemanticModel& model, LogicalOperator child)
    {
        auto op = model.hasFilterStep() ? LogicalOperator{TypedLogicalOperator<SemanticFilterLogicalOperator>{model}}
                                        : LogicalOperator{TypedLogicalOperator<SemanticMapLogicalOperator>{model}};
        if (model.getConfig().execution == SemanticExecution::ASYNCHRONOUS)
        {
            auto traits = op.getTraitSet();
            traits.insert(AsyncExecutionTrait{model.hasFilterStep() ? "SemanticFilter" : "SemanticMap", {}, 4, 10, 64, true});
            op = op.withTraitSet(std::move(traits));
        }
        return op.withChildrenUnsafe({std::move(child)});
    }

    LogicalOperator source()
    {
        return LogicalOperator{utils.createSource(
            "fusionSource" + std::to_string(sources++), Schema<UnqualifiedUnboundField, Ordered>{field("description"), field("title")})};
    }

    static LogicalPlan planWithRoots(std::vector<LogicalOperator> roots)
    {
        return LogicalPlan{
            QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), std::move(roots)};
    }

    static LogicalOperator fuse(LogicalOperator root)
    {
        return SemanticFusionRule{}.apply(planWithRoots({std::move(root)})).getRootOperators().at(0);
    }

    static std::string modelNameOf(const LogicalOperator& op)
    {
        if (const auto map = op.tryGetAs<SemanticMapLogicalOperator>())
        {
            return map->get().getModel().getName();
        }
        if (const auto filter = op.tryGetAs<SemanticFilterLogicalOperator>())
        {
            return filter->get().getModel().getName();
        }
        return "<not semantic>";
    }

    size_t sources = 0;
};

TEST_F(SemanticFusionRuleTest, FusesEveryPairKind)
{
    struct Case
    {
        std::string lower;
        std::string upper;
        bool expectFilter;
    };

    for (const auto& [lowerOutput, upperOutput, expectFilter] :
         std::vector<Case>{{"a", "b", false}, {"", "", true}, {"a", "", true}, {"", "b", true}})
    {
        SCOPED_TRACE(lowerOutput + "/" + upperOutput);
        const auto suffix = std::to_string(sources);
        const auto lower = model("lower" + suffix, lowerOutput);
        const auto upper = model("upper" + suffix, upperOutput);
        const auto src = source();

        const auto root = fuse(resolved(upper, resolved(lower, src)));

        EXPECT_EQ(modelNameOf(root), "lower" + suffix + "+upper" + suffix);
        EXPECT_EQ(root.tryGetAs<SemanticFilterLogicalOperator>().has_value(), expectFilter);
        EXPECT_EQ(root.tryGetAs<SemanticMapLogicalOperator>().has_value(), !expectFilter);
        ASSERT_EQ(root.getChildren().size(), 1);
        EXPECT_EQ(root.getChildren().at(0), src);
        /// The fused operator infers the same schema the pair would.
        const auto schema = root.withInferredSchema().getOutputSchema();
        EXPECT_EQ(schema.size(), 2 + (lowerOutput.empty() ? 0 : 1) + (upperOutput.empty() ? 0 : 1));
    }
}

TEST_F(SemanticFusionRuleTest, ChainsCollapseIntoOneOperator)
{
    const auto src = source();
    const auto root = fuse(resolved(model("c", "y"), resolved(model("b"), resolved(model("a", "x"), src))));

    EXPECT_EQ(modelNameOf(root), "a+b+c");
    ASSERT_TRUE(root.tryGetAs<SemanticFilterLogicalOperator>().has_value());
    const auto& steps = root.getAs<SemanticFilterLogicalOperator>()->getModel().getConfig().steps;
    ASSERT_EQ(steps.size(), 3);
    EXPECT_EQ(steps[1].outputColumn, "__filter_0");
    EXPECT_EQ(root.getChildren().at(0), src);
}

/// The spine above a fused pair is relinked, so an operator that does not fuse sees the fused one.
TEST_F(SemanticFusionRuleTest, RelinksOperatorsAboveTheFusedPair)
{
    const auto src = source();
    const auto pair = resolved(model("b"), resolved(model("a"), src));
    const auto top = resolved(model("top", "summary", [](auto& config) { config.fusion = false; }), pair);

    const auto root = fuse(top);

    EXPECT_EQ(modelNameOf(root), "top");
    EXPECT_EQ(modelNameOf(root.getChildren().at(0)), "a+b");
    EXPECT_EQ(root.getChildren().at(0).getChildren().at(0), src);
}

TEST_F(SemanticFusionRuleTest, RefusesWhatCannotBeFused)
{
    const auto refused = [this](const std::string& description, const RegisteredSemanticModel& lower, const RegisteredSemanticModel& upper)
    {
        SCOPED_TRACE(description);
        const auto root = fuse(resolved(upper, resolved(lower, source())));
        EXPECT_EQ(modelNameOf(root), upper.getName());
        EXPECT_EQ(modelNameOf(root.getChildren().at(0)), lower.getName());
    };

    refused("fusion off below", model("off1", {}, [](auto& config) { config.fusion = false; }), model("on1"));
    refused("fusion off above", model("on2"), model("off2", {}, [](auto& config) { config.fusion = false; }));
    refused("other endpoint", model("e1"), model("e2", {}, [](auto& config) { config.endpoint = "label:true"; }));
    refused("other model", model("n1"), model("n2", {}, [](auto& config) { config.modelName = "other"; }));
    refused("other dataset prompt", model("d1"), model("d2", {}, [](auto& config) { config.datasetPrompt = "reviews"; }));
    refused("other payload format", model("p1"), model("p2", {}, [](auto& config) { config.payloadFormat = PayloadFormat::JSON_OBJECT; }));
    refused("other retries", model("r1"), model("r2", {}, [](auto& config) { config.maxRetries = 5; }));
    refused("other execution", model("x1"), model("x2", {}, [](auto& config) { config.execution = SemanticExecution::ASYNCHRONOUS; }));
    refused("colliding outputs", model("c1", "same"), model("c2", "same"));

    /// Same fields, different order: a different prompt payload.
    SemanticModelConfig config;
    config.endpoint = "echo";
    config.modelName = "mock-model";
    config.backend = "mock";
    config.fusion = true;
    config.steps
        = {SemanticStep{.kind = SemanticStep::Kind::FILTER, .prompt = "p", .outputColumn = {}, .outputValues = {}, .defaultValue = {}}};
    catalog.registerModel(
        "swapped", config, SemanticModelSchema{.inputs = SemanticFieldList{field("title"), field("description")}, .outputs = {}});
    refused("input order", model("i1"), catalog.load("swapped"));
}

/// A child feeding two parents cannot be folded into one of them without running it twice.
TEST_F(SemanticFusionRuleTest, RefusesASharedChild)
{
    const auto shared = resolved(model("shared"), source());
    const auto left = resolved(model("left"), shared);
    const auto right = resolved(model("right"), shared);

    const auto roots = SemanticFusionRule{}.apply(planWithRoots({left, right})).getRootOperators();

    ASSERT_EQ(roots.size(), 2);
    EXPECT_EQ(modelNameOf(roots.at(0)), "left");
    EXPECT_EQ(modelNameOf(roots.at(1)), "right");
    EXPECT_EQ(roots.at(0).getChildren().at(0), roots.at(1).getChildren().at(0));
}

/// The markers on the old operators describe their own step lists; the fused operator needs one
/// built from the fused model, or the executor would send only half the prompt.
TEST_F(SemanticFusionRuleTest, RebuildsTheAsyncMarker)
{
    const auto async = [](SemanticModelConfig& config) { config.execution = SemanticExecution::ASYNCHRONOUS; };
    const auto root = fuse(resolved(model("f", {}, async), resolved(model("m", "sentiment", async), source())));

    const auto trait = root.getTraitSet().tryGet<AsyncExecutionTrait>();
    ASSERT_TRUE(trait.has_value());
    EXPECT_EQ(trait.value()->executorType, "SemanticFilter");
    EXPECT_EQ(trait.value()->batchSize, 4);
    const auto payload = decodeSemanticMapPayload(trait.value()->config.at(std::string{SemanticMapConfigKey}));
    ASSERT_EQ(payload.config.steps.size(), 2);
    EXPECT_EQ(payload.config.steps[0].kind, SemanticStep::Kind::MAP);
    EXPECT_EQ(payload.config.steps[1].kind, SemanticStep::Kind::FILTER);
    EXPECT_EQ(payload.outputFields, (std::vector<std::string>{"SENTIMENT"}));
    EXPECT_EQ(payload.inputFields, (std::vector<std::string>{"DESCRIPTION", "TITLE"}));
}

TEST_F(SemanticFusionRuleTest, SynchronousFusionCarriesNoMarker)
{
    const auto root = fuse(resolved(model("b"), resolved(model("a"), source())));
    EXPECT_FALSE(root.getTraitSet().tryGet<AsyncExecutionTrait>().has_value());
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
