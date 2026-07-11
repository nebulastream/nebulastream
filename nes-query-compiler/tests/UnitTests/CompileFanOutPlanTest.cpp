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

#include <memory>
#include <set>
#include <utility>
#include <variant>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <CompiledQueryPlan.hpp>
#include <QueryCompiler.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{
namespace
{

QueryId randomQueryId()
{
    return QueryId::createLocal(LocalQueryId(generateUUID()));
}

/// Compiles a hand-built, fully bound DAG LogicalPlan (two sinks sharing one selection) end-to-end
/// through the QueryCompiler and verifies the fan-out shape of the CompiledQueryPlan.
/// The plan is built WITHOUT the optimizer: optimizer rules rebuild operators and thereby regenerate
/// operator ids, which would split the shared subplan into independent copies (see fan-out PoC notes).
class CompileFanOutPlanTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("CompileFanOutPlanTest.log", LogLevel::LOG_DEBUG); }

    static Schema createSchema()
    {
        Schema schema;
        schema.addField("test.id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField("test.value", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return schema;
    }

    LogicalOperator makeBoundSource()
    {
        auto schema = createSchema();
        auto descriptor = sourceCatalog.getInlineSource("File", schema, Host("localhost"), {{"type", "CSV"}}, {{"file_path", "/dev/null"}});
        EXPECT_TRUE(descriptor.has_value());
        return LogicalOperator{SourceDescriptorLogicalOperator{std::move(descriptor.value())}}
            /// NOLINT(bugprone-unchecked-optional-access)
            .withTraitSet(TraitSet{OutputOriginIdsTrait{std::vector{OriginId(1)}}, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});
    }

    LogicalOperator makeBoundSink(const LogicalOperator& child)
    {
        auto descriptor = sinkCatalog.getInlineSink(createSchema(), "Print", Host("localhost"), {{"output_format", "CSV"}}, {});
        EXPECT_TRUE(descriptor.has_value());
        return LogicalOperator{SinkLogicalOperator{descriptor.value()}} /// NOLINT(bugprone-unchecked-optional-access)
            .withChildren({child})
            .withTraitSet(TraitSet{MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});
    }

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
};

TEST_F(CompileFanOutPlanTest, TwoSinksSharingSelectionCompileToFanOutPipeline)
{
    const auto source = makeBoundSource();

    /// Shared selection (test.id >= 0), fully bound: children first, then schema inference, then layout trait.
    auto selection = LogicalOperator{SelectionLogicalOperator{GreaterEqualsLogicalFunction(
                                         FieldAccessLogicalFunction("test.id"),
                                         ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UINT64), "0"))}}
                         .withChildren({source});
    selection = selection.withInferredSchema({source.getOutputSchema()});
    selection = selection.withTraitSet(TraitSet{MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT}});

    ASSERT_TRUE(getTrait<OutputOriginIdsTrait>(source.getTraitSet()).has_value());
    ASSERT_EQ(selection.getChildren().size(), 1U);
    ASSERT_TRUE(getTrait<OutputOriginIdsTrait>(selection.getChildren()[0].getTraitSet()).has_value());

    /// Both sinks share the SAME selection instance: this makes the logical plan a DAG.
    const auto sinkA = makeBoundSink(selection);
    const auto sinkB = makeBoundSink(selection);
    auto plan = LogicalPlan(randomQueryId(), {sinkA, sinkB});

    auto compiler = QueryCompilation::QueryCompiler{QueryExecutionConfiguration{}};
    const auto compiled = compiler.compileQuery(std::make_unique<QueryCompilation::QueryCompilationRequest>(
        QueryCompilation::QueryCompilationRequest{.queryPlan = std::move(plan)}));

    ASSERT_TRUE(compiled);
    /// One source (the shared subplan must NOT be duplicated), two sinks.
    ASSERT_EQ(compiled->sources.size(), 1U);
    ASSERT_EQ(compiled->sinks.size(), 2U);
    /// Shared pipeline (scan+selection+emit) + one CSV formatting pipeline per sink branch.
    ASSERT_EQ(compiled->pipelines.size(), 3U);

    ASSERT_EQ(compiled->sources[0].successors.size(), 1U);
    const auto sharedPipeline = compiled->sources[0].successors[0].lock();
    ASSERT_TRUE(sharedPipeline);
    ASSERT_EQ(sharedPipeline->successors.size(), 2U);

    std::set<PipelineId> branchIds;
    for (const auto& weakBranch : sharedPipeline->successors)
    {
        const auto branch = weakBranch.lock();
        ASSERT_TRUE(branch);
        /// The formatting pipelines end at the sinks, which are not ExecutablePipelines in the CompiledQueryPlan.
        EXPECT_TRUE(branch->successors.empty());
        branchIds.insert(branch->id);
    }
    EXPECT_EQ(branchIds.size(), 2U);

    /// Each sink hangs off exactly one of the two branches, and not the same one.
    std::set<PipelineId> sinkPredecessorIds;
    for (const auto& sink : compiled->sinks)
    {
        ASSERT_EQ(sink.predecessor.size(), 1U);
        const auto* weakPredecessor = std::get_if<std::weak_ptr<ExecutablePipeline>>(&sink.predecessor[0]);
        ASSERT_NE(weakPredecessor, nullptr);
        const auto predecessor = weakPredecessor->lock();
        ASSERT_TRUE(predecessor);
        sinkPredecessorIds.insert(predecessor->id);
    }
    EXPECT_EQ(sinkPredecessorIds, branchIds);
}

}
}
