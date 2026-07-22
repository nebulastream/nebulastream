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

/// Verifies the fix for "Lower multi-sink logical plans into a shared physical operator DAG": the tests
/// optimize a single-sink tree with the real RuleBasedOptimizer, graft a second sink root onto the
/// optimized plan (sharing an operator instance — the deployment model where additional statistic sinks
/// are attached to an already-optimized plan), compile through the QueryCompiler, and assert the
/// CompiledQueryPlan shape at the lowering level (sink and source counts). Without the fix,
/// - apply() passed only newRootOperators[0] to the PhysicalPlanBuilder, silently dropping further sinks,
/// - lowerOperatorRecursively had no memoization, so an operator shared by multiple parents was lowered
///   once per path, duplicating the whole upstream subplan including the source.
/// The internal pipeline structure of the shared part (one emit, distinct successor pipelines) is NOT
/// asserted here — that is the separate pipelining fan-out issue.
/// The graft happens post-optimizer because the per-rule optimizer does not yet preserve multi-root
/// sharing (#1753: shared operators are rewritten once per root with fresh ids).
/// That graft has a cost worth knowing about when reading the assertions: the second sink's TraitSet is
/// copied wholesale from the optimizer-bound first sink, so it carries traits that were computed for a
/// *different* sink operator (OutputOriginIdsTrait, PlacementTrait, ...). These tests therefore pin a
/// trait configuration that no real plan would ever have. Once #1753 lands, build the two-sink plan
/// before the optimizer and drop the TraitSet copy, so the tests cover a plan the system can produce.

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{

class MultiRootLoweringTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("MultiRootLoweringTest.log", LogLevel::LOG_DEBUG); }

    static Schema<UnqualifiedUnboundField, Ordered> createSchema()
    {
        std::vector<UnqualifiedUnboundField> fields;
        fields.emplace_back(Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64));
        fields.emplace_back(Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return Schema<UnqualifiedUnboundField, Ordered>{fields};
    }

    TypedLogicalOperator<SourceDescriptorLogicalOperator> createSource(const std::string& name)
    {
        const auto logicalSource = sourceCatalog.addLogicalSource(Identifier::parse(name), createSchema());
        EXPECT_TRUE(logicalSource.has_value());
        const std::unordered_map<Identifier, std::string> sourceConfig{{Identifier::parse("FILE_PATH"), "/dev/null"}};
        const std::unordered_map<Identifier, std::string> parserConfig{{Identifier::parse("TYPE"), "CSV"}};
        auto descriptor = sourceCatalog.addPhysicalSource(
            logicalSource.value(), Identifier::parse("file"), Host{"localhost"}, sourceConfig, parserConfig);
        EXPECT_TRUE(descriptor.has_value());
        return SourceDescriptorLogicalOperator::create(std::move(descriptor.value()));
    }

    SinkDescriptor createSinkDescriptor(const std::string& name)
    {
        const std::unordered_map<Identifier, std::string> sinkConfig{
            {Identifier::parse("FILE_PATH"), "/dev/null"}, {Identifier::parse("OUTPUT_FORMAT"), "CSV"}};
        auto descriptor = sinkCatalog.addSinkDescriptor(
            Identifier::parse(name), createSchema(), Identifier::parse("file"), Host{"localhost"}, sinkConfig, {});
        EXPECT_TRUE(descriptor.has_value());
        return descriptor.value();
    }

    /// Builds source -> selection -> sink and runs it through the real optimizer to obtain a fully
    /// bound (traits attached) single-root plan.
    LogicalPlan makeOptimizedSingleSinkPlan(const std::string& sourceName, const std::string& sinkName)
    {
        const auto source = createSource(sourceName);
        const auto predicate = GreaterEqualsLogicalFunction{
            FieldAccessLogicalFunction{source.getOutputSchema()[Identifier::parse("id")].value()},
            ConstantValueLogicalFunction{DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}, "0"}};
        const auto selection = SelectionLogicalOperator::create(source, predicate);
        const auto sink = SinkLogicalOperator::create(selection, createSinkDescriptor(sinkName));
        auto plan
            = LogicalPlan{QueryId::create(LocalQueryId{LocalQueryId::INVALID}, DistributedQueryId{DistributedQueryId::INVALID}), {sink}};
        return RuleBasedOptimizer{QueryOptimizerConfiguration{}}.optimize(std::move(plan));
    }

    static LogicalOperator leafOf(LogicalOperator op)
    {
        while (!op.getChildren().empty())
        {
            op = op.getChildren()[0];
        }
        return op;
    }

    static std::unique_ptr<CompiledQueryPlan> compile(LogicalPlan plan)
    {
        auto compiler = QueryCompilation::QueryCompiler{QueryExecutionConfiguration{}};
        return compiler.compileQuery(std::make_unique<QueryCompilation::QueryCompilationRequest>(
            QueryCompilation::QueryCompilationRequest{.queryPlan = std::move(plan)}));
    }

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
};

/// Two sink roots sharing one optimized subplan: the compiled plan must contain BOTH sinks (and one
/// source, i.e. the shared subplan lowered exactly once).
TEST_F(MultiRootLoweringTest, SecondSinkRootIsNotDropped)
{
    const auto optimized = makeOptimizedSingleSinkPlan("multiroot", "multiroot_sink1");
    ASSERT_EQ(optimized.getRootOperators().size(), 1U);
    const auto firstSink = optimized.getRootOperators()[0];
    ASSERT_EQ(firstSink.getChildren().size(), 1U);

    /// Graft a second sink sharing the first sink's child; the sink-level traits are copied from the
    /// optimizer-bound first sink, everything below is shared as-is. See the #1753 note in the file
    /// header: this copy is scaffolding and should go once the optimizer preserves multi-root sharing.
    const auto secondSink
        = LogicalOperator{SinkLogicalOperator::create(firstSink.getChildren()[0], createSinkDescriptor("multiroot_sink2"))}.withTraitSet(
            firstSink.getTraitSet());
    auto plan = LogicalPlan{optimized.getQueryId(), {firstSink, secondSink}};

    const auto compiled = compile(std::move(plan));

    ASSERT_TRUE(compiled);
    EXPECT_EQ(compiled->sinks.size(), 2U) << "the second sink root was silently dropped by LowerToPhysicalOperators::apply";
    EXPECT_EQ(compiled->sources.size(), 1U) << "the shared subplan (incl. the source) was lowered once per root";
}

/// Two sink roots sharing the SAME source instance (second sink grafted directly onto the source leaf):
/// the shared source must be lowered exactly once — otherwise the input is physically read twice.
TEST_F(MultiRootLoweringTest, SharedSourceIsLoweredOnce)
{
    const auto optimized = makeOptimizedSingleSinkPlan("sharedsource", "sharedsource_sink1");
    ASSERT_EQ(optimized.getRootOperators().size(), 1U);
    const auto firstSink = optimized.getRootOperators()[0];
    const auto source = leafOf(firstSink);
    ASSERT_TRUE(source.tryGetAs<SourceDescriptorLogicalOperator>().has_value());

    /// Same TraitSet-copy scaffolding as above; see the #1753 note in the file header.
    const auto secondSink = LogicalOperator{SinkLogicalOperator::create(source, createSinkDescriptor("sharedsource_sink2"))}.withTraitSet(
        firstSink.getTraitSet());
    auto plan = LogicalPlan{optimized.getQueryId(), {firstSink, secondSink}};

    const auto compiled = compile(std::move(plan));

    ASSERT_TRUE(compiled);
    EXPECT_EQ(compiled->sinks.size(), 2U) << "the second sink root was silently dropped by LowerToPhysicalOperators::apply";
    EXPECT_EQ(compiled->sources.size(), 1U)
        << "the source shared by both sink roots was lowered once per path — the input would be read twice";
}

}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
