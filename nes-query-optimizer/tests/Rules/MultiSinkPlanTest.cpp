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

/// Tests that all optimization rules handle multi-sink plans (#1753). Multi-sink plans share operator instances
/// between sink roots, so every rule must rewrite each unique operator exactly once and reuse the rewritten instance
/// for every parent. Sharing is observable through operator ids: the same id reachable through two roots.

#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <gtest/gtest.h>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/AnonymousSourceBindingRule.hpp>
#include <Rules/Semantic/CalcTargetOrderRule.hpp>
#include <Rules/Semantic/InferModelResolutionRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/OriginIdInferenceRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Rules/Static/DecideFieldMappings.hpp>
#include <Rules/Static/DecideFieldOrder.hpp>
#include <Rules/Static/DecideJoinTypesRule.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <Rules/Static/PredicatePushdownRule.hpp>
#include <Rules/Static/ProjectionPushdownRule.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <Rules/Static/RedundantUnionRemovalRule.hpp>
#include <Rules/Static/WatermarkAssignerPushdownRule.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/JoinImplementationTypeTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <OptimizerTestUtils.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{

void expectSharedRootChild(const LogicalPlan& plan)
{
    const auto roots = plan.getRootOperators();
    ASSERT_EQ(roots.size(), 2);
    EXPECT_EQ(roots.at(0).getChildren().at(0).getId(), roots.at(1).getChildren().at(0).getId());
}
}

class MultiSinkPlanTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("MultiSinkPlanTest.log", LogLevel::LOG_DEBUG); }

    OptimizerTestUtils utils;

    /// source(a, b) <- selection(a == 0) <- {sink1, sink2}: the selection (and everything below) is shared.
    LogicalPlan createSharedChainPlan(const std::string& prefix)
    {
        auto source = utils.createSource(prefix + "_src", {"a", "b"});
        auto selection = SelectionLogicalOperator::create(source, OptimizerTestUtils::equalsZero(source, "a"));
        auto sink1 = utils.createSink(selection, prefix + "_sink1", {"a", "b"});
        auto sink2 = utils.createSink(selection, prefix + "_sink2", {"a", "b"});
        return utils.createPlan({sink1, sink2});
    }
};

TEST_F(MultiSinkPlanTest, DecideMemoryLayoutPreservesSharing)
{
    const auto result = DecideMemoryLayoutRule{}.apply(createSharedChainPlan("memory_layout"));

    expectSharedRootChild(result);
    const auto operators = OptimizerTestUtils::collectOperators(result);
    EXPECT_EQ(operators.size(), 4);
    for (const auto& op : operators)
    {
        EXPECT_TRUE(hasTrait<MemoryLayoutTypeTrait>(op.getTraitSet())) << "missing memory layout trait on " << op;
    }
}

TEST_F(MultiSinkPlanTest, DecideJoinTypesPreservesSharing)
{
    const auto result = DecideJoinTypesRule{StreamJoinStrategy::OPTIMIZER_CHOOSES}.apply(createSharedChainPlan("join_types"));

    expectSharedRootChild(result);
    const auto operators = OptimizerTestUtils::collectOperators(result);
    EXPECT_EQ(operators.size(), 4);
    for (const auto& op : operators)
    {
        EXPECT_TRUE(hasTrait<JoinImplementationTypeTrait>(op.getTraitSet())) << "missing join implementation trait on " << op;
    }
}

TEST_F(MultiSinkPlanTest, DecideFieldMappingsPreservesSharing)
{
    const auto result = DecideFieldMappings{}.apply(createSharedChainPlan("field_mappings"));

    expectSharedRootChild(result);
    const auto operators = OptimizerTestUtils::collectOperators(result);
    EXPECT_EQ(operators.size(), 4);
    for (const auto& op : operators)
    {
        EXPECT_TRUE(hasTrait<FieldMappingTrait>(op.getTraitSet())) << "missing field mapping trait on " << op;
    }
}

TEST_F(MultiSinkPlanTest, OriginIdInferenceAssignsSharedOperatorsOneOriginId)
{
    const auto result = OriginIdInferenceRule{}.apply(createSharedChainPlan("origin_ids"));

    expectSharedRootChild(result);
    const auto roots = result.getRootOperators();

    /// The shared source is the only origin id assigner; it must have been assigned exactly one origin id
    /// that both sinks observe.
    std::unordered_set<OriginId> allOriginIds;
    for (const auto& op : OptimizerTestUtils::collectOperators(result))
    {
        const auto traitOpt = getTrait<OutputOriginIdsTrait>(op.getTraitSet());
        ASSERT_TRUE(traitOpt.has_value()) << "missing origin ids trait on " << op;
        allOriginIds.insert(traitOpt.value().get().begin(), traitOpt.value().get().end());
    }
    EXPECT_EQ(allOriginIds.size(), 1);

    const auto sink1Ids = getTrait<OutputOriginIdsTrait>(roots.at(0).getTraitSet()).value().get();
    const auto sink2Ids = getTrait<OutputOriginIdsTrait>(roots.at(1).getTraitSet()).value().get();
    EXPECT_TRUE(sink1Ids == sink2Ids);
}

TEST_F(MultiSinkPlanTest, RedundantUnionRemovalRemovesSharedUnionOnce)
{
    auto source = utils.createSource("redundant_union_src", {"a", "b"});
    auto unionOp = UnionLogicalOperator::create(std::vector<LogicalOperator>{source});
    auto sink1 = utils.createSink(unionOp, "redundant_union_sink1", {"a", "b"});
    auto sink2 = utils.createSink(unionOp, "redundant_union_sink2", {"a", "b"});

    const auto result = RedundantUnionRemovalRule{}.apply(utils.createPlan({sink1, sink2}));

    expectSharedRootChild(result);
    const auto roots = result.getRootOperators();
    EXPECT_TRUE(roots.at(0).getChildren().at(0).tryGetAs<SourceDescriptorLogicalOperator>().has_value());
    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 3);
}

TEST_F(MultiSinkPlanTest, RedundantProjectionRemovalRemovesSharedProjectionOnce)
{
    auto source = utils.createSource("redundant_proj_src", {"a", "b"});
    auto projection = ProjectionLogicalOperator::create(
        source,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("a"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("a")].value()}},
            {Identifier::parse("b"), FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("b")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});
    auto sink1 = utils.createSink(projection, "redundant_proj_sink1", {"a", "b"});
    auto sink2 = utils.createSink(projection, "redundant_proj_sink2", {"a", "b"});

    const auto result = RedundantProjectionRemovalRule{}.apply(utils.createPlan({sink1, sink2}));

    expectSharedRootChild(result);
    const auto roots = result.getRootOperators();
    EXPECT_TRUE(roots.at(0).getChildren().at(0).tryGetAs<SourceDescriptorLogicalOperator>().has_value());
    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 3);
}

TEST_F(MultiSinkPlanTest, DecideFieldOrderStampsAgreedTargetOrderOnSharedChild)
{
    const auto result = DecideFieldOrder{}.apply(createSharedChainPlan("field_order"));

    expectSharedRootChild(result);
    const auto operators = OptimizerTestUtils::collectOperators(result);
    EXPECT_EQ(operators.size(), 4);
    for (const auto& op : operators)
    {
        EXPECT_TRUE(hasTrait<FieldOrderingTrait>(op.getTraitSet())) << "missing field ordering trait on " << op;
    }

    /// Both sinks demand the same target order, so the shared child carries exactly that order.
    const auto sharedChild = result.getRootOperators().at(0).getChildren().at(0);
    const auto orderedFields = sharedChild.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields();
    EXPECT_EQ(orderedFields, utils.createSchema({"a", "b"}));
}

TEST_F(MultiSinkPlanTest, DecideFieldOrderThrowsOnConflictingTargetOrders)
{
    auto source = utils.createSource("field_order_conflict_src", {"a", "b"});
    auto selection = SelectionLogicalOperator::create(source, OptimizerTestUtils::equalsZero(source, "a"));
    auto sink1 = utils.createSink(selection, "field_order_conflict_sink1", {"a", "b"});
    auto sink2 = utils.createSink(selection, "field_order_conflict_sink2", {"b", "a"});
    const auto plan = utils.createPlan({sink1, sink2});

    ASSERT_EXCEPTION_ERRORCODE(std::ignore = DecideFieldOrder{}.apply(plan), ErrorCode::UnsupportedQuery);
}

TEST_F(MultiSinkPlanTest, DecideFieldOrderPropagatesSharedTargetOrderIntoOtherSinkBranch)
{
    /// A shared operator has a single physical output order, so an order one sink imposes on it legitimately propagates
    /// into the heuristic order of operators in another sink's branch that also read it.
    /// Topology: source(a,b) <- shared <- {sink1(order b,a); middle <- top <- sink2(order a,b)}.
    /// `shared` is the direct child of sink1 (so sink1 pins it to [b,a]) and, via middle/top, also feeds sink2.
    /// `middle` is not a direct sink child, so it receives a heuristic order derived from `shared`.
    auto source = utils.createSource("shared_target_order_src", {"a", "b"});
    auto shared = SelectionLogicalOperator::create(source, OptimizerTestUtils::equalsZero(source, "a"));
    auto sink1 = utils.createSink(shared, "shared_target_order_sink1", {"b", "a"});
    auto middle = SelectionLogicalOperator::create(shared, OptimizerTestUtils::equalsZero(shared, "b"));
    auto top = SelectionLogicalOperator::create(middle, OptimizerTestUtils::equalsZero(middle, "a"));
    auto sink2 = utils.createSink(top, "shared_target_order_sink2", {"a", "b"});

    const auto result = DecideFieldOrder{}.apply(utils.createPlan({sink1, sink2}));

    const auto roots = result.getRootOperators();
    ASSERT_EQ(roots.size(), 2);

    /// The shared operator stays a single instance reachable directly from sink1 and via top -> middle from sink2.
    const auto sharedFromSink1 = roots.at(0).getChildren().at(0);
    const auto middleRewritten = roots.at(1).getChildren().at(0).getChildren().at(0);
    const auto sharedFromSink2 = middleRewritten.getChildren().at(0);
    EXPECT_EQ(sharedFromSink1.getId(), sharedFromSink2.getId());

    /// sink1 pins the shared operator to [b, a]; `middle` in sink2's branch inherits exactly that order rather than the
    /// [a, b] it would carry in isolation.
    const auto reversed = utils.createSchema({"b", "a"});
    EXPECT_EQ(sharedFromSink1.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields(), reversed);
    EXPECT_EQ(middleRewritten.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields(), reversed);

    /// source, shared, sink1, middle, top, sink2 — no rule duplicated the shared subtree.
    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 6);
}

TEST_F(MultiSinkPlanTest, PredicatePushdownKeepsBranchPredicateAboveSharedOperator)
{
    /// BEFORE: source <- sharedSelection(b == 0) <- {branchSelection(a == 0) <- sink1, sink2}
    /// AFTER:  the branch predicate must stay above the shared boundary; the shared subtree is rewritten once.
    auto source = utils.createSource("pred_pushdown_src", {"a", "b"});
    auto sharedSelection = SelectionLogicalOperator::create(source, OptimizerTestUtils::equalsZero(source, "b"));
    auto branchSelection = SelectionLogicalOperator::create(sharedSelection, OptimizerTestUtils::equalsZero(sharedSelection, "a"));
    auto sink1 = utils.createSink(branchSelection, "pred_pushdown_sink1", {"a", "b"});
    auto sink2 = utils.createSink(sharedSelection, "pred_pushdown_sink2", {"a", "b"});

    const auto result = PredicatePushdownRule{}.apply(utils.createPlan({sink1, sink2}));

    const auto roots = result.getRootOperators();
    ASSERT_EQ(roots.size(), 2);

    /// Branch 1: sink <- selection(a == 0) <- selection(b == 0) <- source
    const auto branch1Selection = roots.at(0).getChildren().at(0);
    ASSERT_TRUE(branch1Selection.tryGetAs<SelectionLogicalOperator>().has_value());
    const auto branch1Shared = branch1Selection.getChildren().at(0);
    ASSERT_TRUE(branch1Shared.tryGetAs<SelectionLogicalOperator>().has_value());

    /// Branch 2: sink <- selection(b == 0) <- source, without the branch predicate
    const auto branch2Shared = roots.at(1).getChildren().at(0);
    ASSERT_TRUE(branch2Shared.tryGetAs<SelectionLogicalOperator>().has_value());
    EXPECT_EQ(branch1Shared.getId(), branch2Shared.getId());
    EXPECT_TRUE(branch2Shared.getChildren().at(0).tryGetAs<SourceDescriptorLogicalOperator>().has_value());

    /// sink1, sink2, branch selection, shared selection, source
    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 5);
}

TEST_F(MultiSinkPlanTest, WatermarkAssignerPushdownStopsAtSharedOperator)
{
    /// BEFORE: source <- sharedSelection <- {eventTimeAssigner <- sink1, sink2}
    /// AFTER:  the assigner must not descend into the shared subtree, where sink2 would observe it.
    auto source = utils.createSource("watermark_src", {"a", "b"});
    auto sharedSelection = SelectionLogicalOperator::create(source, OptimizerTestUtils::equalsZero(source, "a"));
    auto eventTime = EventTimeWatermarkAssignerLogicalOperator::create(
        sharedSelection,
        FieldAccessLogicalFunction{sharedSelection.getOutputSchema()[Identifier::parse("a")].value()},
        Windowing::TimeUnit{0});
    auto sink1 = utils.createSink(eventTime, "watermark_sink1", {"a", "b"});
    auto sink2 = utils.createSink(sharedSelection, "watermark_sink2", {"a", "b"});

    const auto result = WatermarkAssignerPushdownRule{}.apply(utils.createPlan({sink1, sink2}));

    const auto roots = result.getRootOperators();
    ASSERT_EQ(roots.size(), 2);

    /// Branch 1: sink <- eventTimeAssigner <- selection <- source
    const auto branch1Assigner = roots.at(0).getChildren().at(0);
    ASSERT_TRUE(branch1Assigner.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value());
    const auto branch1Shared = branch1Assigner.getChildren().at(0);
    ASSERT_TRUE(branch1Shared.tryGetAs<SelectionLogicalOperator>().has_value());

    /// Branch 2: sink <- selection <- source, without any watermark assigner
    const auto branch2Shared = roots.at(1).getChildren().at(0);
    EXPECT_EQ(branch1Shared.getId(), branch2Shared.getId());
    ASSERT_TRUE(branch2Shared.getChildren().at(0).tryGetAs<SourceDescriptorLogicalOperator>().has_value());

    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 5);
}

TEST_F(MultiSinkPlanTest, ProjectionPushdownDoesNotNarrowSharedSubtree)
{
    /// BEFORE: source(a, b) <- sharedSelection <- {projection(a) <- sink1(a), sink2(a, b)}
    /// AFTER:  sink1 only requires "a", but the shared subtree must keep the full schema for sink2.
    auto source = utils.createSource("proj_pushdown_src", {"a", "b"});
    auto sharedSelection = SelectionLogicalOperator::create(source, OptimizerTestUtils::equalsZero(source, "a"));
    auto projection = ProjectionLogicalOperator::create(
        sharedSelection,
        std::vector<std::pair<Identifier, LogicalFunction>>{
            {Identifier::parse("a"), FieldAccessLogicalFunction{sharedSelection.getOutputSchema()[Identifier::parse("a")].value()}}},
        ProjectionLogicalOperator::Asterisk{false});
    auto sink1 = utils.createSink(projection, "proj_pushdown_sink1", {"a"});
    auto sink2 = utils.createSink(sharedSelection, "proj_pushdown_sink2", {"a", "b"});

    const auto result = ProjectionPushdownRule{}.apply(utils.createPlan({sink1, sink2}));

    const auto roots = result.getRootOperators();
    ASSERT_EQ(roots.size(), 2);

    const auto branch1Projection = roots.at(0).getChildren().at(0);
    ASSERT_TRUE(branch1Projection.tryGetAs<ProjectionLogicalOperator>().has_value());
    const auto branch1Shared = branch1Projection.getChildren().at(0);
    const auto branch2Shared = roots.at(1).getChildren().at(0);
    EXPECT_EQ(branch1Shared.getId(), branch2Shared.getId());

    /// The shared subtree still produces both fields and contains no narrowing projection.
    EXPECT_TRUE(branch2Shared.getOutputSchema()[Identifier::parse("a")].has_value());
    EXPECT_TRUE(branch2Shared.getOutputSchema()[Identifier::parse("b")].has_value());
    for (const auto& op : OptimizerTestUtils::collectOperators(utils.createPlan({roots.at(1)})))
    {
        EXPECT_FALSE(op.tryGetAs<ProjectionLogicalOperator>().has_value()) << "unexpected projection below shared boundary: " << op;
    }
}

TEST_F(MultiSinkPlanTest, TypeInferencePreservesSharing)
{
    const auto result = TypeInferenceRule{}.apply(createSharedChainPlan("type_inference"));

    expectSharedRootChild(result);
    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 4);
}

TEST_F(MultiSinkPlanTest, SinkBindingHandlesMultipleRoots)
{
    /// The sinks already carry descriptors, so binding passes them through and must not touch the shared subtree.
    const auto plan = createSharedChainPlan("sink_binding");
    const auto sharedChildId = plan.getRootOperators().at(0).getChildren().at(0).getId();

    const auto result = SinkBindingRule{std::make_shared<SinkCatalog>()}.apply(plan);

    const auto roots = result.getRootOperators();
    ASSERT_EQ(roots.size(), 2);
    EXPECT_EQ(roots.at(0).getChildren().at(0).getId(), sharedChildId);
    EXPECT_EQ(roots.at(1).getChildren().at(0).getId(), sharedChildId);
}

TEST_F(MultiSinkPlanTest, AnonymousSourceBindingPreservesSharing)
{
    const auto result = AnonymousSourceBindingRule{std::make_shared<SourceCatalog>()}.apply(createSharedChainPlan("anonymous_source"));

    expectSharedRootChild(result);
    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 4);
}

TEST_F(MultiSinkPlanTest, InferModelResolutionPreservesSharing)
{
    const auto result = InferModelResolutionRule{std::make_shared<ModelCatalog>()}.apply(createSharedChainPlan("infer_model"));

    expectSharedRootChild(result);
    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 4);
}

TEST_F(MultiSinkPlanTest, LogicalSourceExpansionExpandsSharedSourceNameOnce)
{
    auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto schema = utils.createSchema({"a", "b"});
    const auto logicalSource = sourceCatalog->addLogicalSource(Identifier::parse("expansion_src"), schema);
    ASSERT_TRUE(logicalSource.has_value());
    const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("FILE_PATH"), "/dev/null"}};
    const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("TYPE"), "CSV"}};
    ASSERT_TRUE(
        sourceCatalog->addPhysicalSource(logicalSource.value(), Identifier::parse("file"), Host{"localhost"}, sourceConfig, parserConfig)
            .has_value());

    const auto sourceName = SourceNameLogicalOperator::create(Identifier::parse("expansion_src"));
    const auto sink1 = SinkLogicalOperator::create(utils.createSinkDescriptor(Identifier::parse("expansion_sink1"), schema))
                           .withChildrenUnsafe({sourceName});
    const auto sink2 = SinkLogicalOperator::create(utils.createSinkDescriptor(Identifier::parse("expansion_sink2"), schema))
                           .withChildrenUnsafe({sourceName});

    const auto result = LogicalSourceExpansionRule{sourceCatalog}.apply(utils.createPlan({sink1, sink2}));

    expectSharedRootChild(result);
    const auto expandedUnion = result.getRootOperators().at(0).getChildren().at(0);
    ASSERT_TRUE(expandedUnion.tryGetAs<UnionLogicalOperator>().has_value());
    ASSERT_EQ(expandedUnion.getChildren().size(), 1);
    EXPECT_TRUE(expandedUnion.getChildren().at(0).tryGetAs<SourceDescriptorLogicalOperator>().has_value());
    /// sink1, sink2, union, physical source
    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 4);
}

TEST_F(MultiSinkPlanTest, CalcTargetOrderInfersOrderForAllSinks)
{
    SinkCatalog sinkCatalog;
    const std::unordered_map<Identifier, std::string> sinkConfig{
        {Identifier::parse("file_path"), "/dev/null"}, {Identifier::parse("output_format"), "CSV"}};
    const auto descriptor1 = sinkCatalog.getAnonymousSink(std::nullopt, Identifier::parse("file"), Host{"localhost"}, sinkConfig, {});
    const auto descriptor2 = sinkCatalog.getAnonymousSink(std::nullopt, Identifier::parse("file"), Host{"localhost"}, sinkConfig, {});
    ASSERT_TRUE(descriptor1.has_value());
    ASSERT_TRUE(descriptor2.has_value());

    auto source = utils.createSource("calc_target_order_src", {"a", "b"});
    auto selection = SelectionLogicalOperator::create(source, OptimizerTestUtils::equalsZero(source, "a"));
    const auto sink1 = SinkLogicalOperator::create(selection, descriptor1.value());
    const auto sink2 = SinkLogicalOperator::create(selection, descriptor2.value());

    const auto result = CalcTargetOrderRule{}.apply(utils.createPlan({sink1, sink2}));

    expectSharedRootChild(result);
    for (const auto& root : result.getRootOperators())
    {
        const auto schemaVariant = root.getAs<SinkLogicalOperator>()->getSinkDescriptor()->getSchema();
        EXPECT_TRUE((std::holds_alternative<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(schemaVariant)))
            << "sink target schema order was not inferred";
    }
    /// The rule only rebinds sink descriptors; the shared subtree must be untouched.
    EXPECT_EQ(result.getRootOperators().at(0).getChildren().at(0).getId(), LogicalOperator{selection}.getId());
}

TEST_F(MultiSinkPlanTest, RuleBasedOptimizerSequencePreservesSharing)
{
    const RuleBasedOptimizer optimizer{QueryOptimizerConfiguration{}};

    const auto result = optimizer.optimize(createSharedChainPlan("full_sequence"));

    expectSharedRootChild(result);
    /// sink1, sink2, the shared selection, and the source — no rule in the sequence may duplicate the shared subtree
    EXPECT_EQ(OptimizerTestUtils::collectOperators(result).size(), 4);
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
