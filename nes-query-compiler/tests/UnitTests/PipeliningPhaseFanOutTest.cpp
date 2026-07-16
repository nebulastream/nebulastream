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

/// Pipeline shapes produced by the pipelining phase for operators with multiple consumers (fan-out) and,
/// as a regression guard, for operators with multiple inputs (fan-in/merge points).
/// All plans are single-root: multi-sink plans additionally require the multi-root lowering/flip (separate
/// issue); the fan-out shapes with one pipeline per sink are covered there once it lands.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
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
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <EmitPhysicalOperator.hpp>
#include <InputFormatterDescriptor.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <PhysicalPlanBuilder.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourceDescriptorPhysicalOperator.hpp>
#include <UnionPhysicalOperator.hpp>

namespace NES
{
namespace
{

QueryId randomQueryId()
{
    return QueryId::createLocal(LocalQueryId(generateUUID()));
}

using PipelineLocation = PhysicalOperatorWrapper::PipelineLocation;

class PipeliningPhaseFanOutTest : public Testing::BaseUnitTest
{
public:
    static constexpr uint64_t BUFFER_SIZE = 4096;

    static void SetUpTestSuite() { Logger::setupLogging("PipeliningPhaseFanOutTest.log", LogLevel::LOG_DEBUG); }

    static Schema<UnqualifiedUnboundField, Ordered> createSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
    }

    std::shared_ptr<PhysicalOperatorWrapper> makeSourceWrapper()
    {
        auto schema = createSchema();
        auto descriptor = sourceCatalog.getInlineSource(
            Identifier::parse("File"),
            schema,
            Host("localhost"),
            {{Identifier::parse(InputFormatterDescriptor::getTypeString()), "CSV"}},
            {{Identifier::parse("file_path"), "/dev/null"}});
        EXPECT_TRUE(descriptor.has_value());
        auto sourceOp = SourceDescriptorPhysicalOperator(
            std::move(descriptor.value()), /// NOLINT(bugprone-unchecked-optional-access)
            OriginId(nextOriginId++));
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{sourceOp}, schema, schema, MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::ROW_LAYOUT, PipelineLocation::SCAN);
    }

    std::shared_ptr<PhysicalOperatorWrapper> makeSinkWrapper() const
    {
        auto schema = createSchema();
        auto descriptor = sinkCatalog.getInlineSink(
            schema, Identifier::parse("Print"), Host("localhost"), {{Identifier::parse("output_format"), "CSV"}}, {});
        EXPECT_TRUE(descriptor.has_value());
        auto sinkOp = SinkPhysicalOperator(descriptor.value()); /// NOLINT(bugprone-unchecked-optional-access)
        /// The real sink lowering (LowerToPhysicalSink) places sinks as INTERMEDIATE; the pipelining
        /// phase identifies them via the operator type, not the pipeline location.
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{sinkOp},
            schema,
            schema,
            MemoryLayoutType::ROW_LAYOUT,
            MemoryLayoutType::ROW_LAYOUT,
            PipelineLocation::INTERMEDIATE);
    }

    static std::shared_ptr<PhysicalOperatorWrapper> makeIntermediateWrapper()
    {
        auto schema = createSchema();
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{UnionPhysicalOperator()},
            schema,
            schema,
            MemoryLayoutType::ROW_LAYOUT,
            MemoryLayoutType::ROW_LAYOUT,
            PipelineLocation::INTERMEDIATE);
    }

    /// Builds the PhysicalPlan for the given sink-rooted wrapper and runs the pipelining phase.
    static std::shared_ptr<PipelinedQueryPlan> pipeline(const std::shared_ptr<PhysicalOperatorWrapper>& sinkRoot)
    {
        auto builder = PhysicalPlanBuilder(randomQueryId());
        builder.addSinkRoot(sinkRoot);
        builder.setOperatorBufferSize(BUFFER_SIZE);
        return QueryCompilation::PipeliningPhase::apply(std::move(builder).finalize());
    }

    /// Counts operators of type T in the pipeline's operator chain.
    template <typename T>
    static size_t countOperators(const Pipeline& pipeline)
    {
        size_t count = 0;
        std::optional<PhysicalOperator> current = pipeline.getRootOperator();
        while (current.has_value())
        {
            if (current->tryGet<T>())
            {
                ++count;
            }
            current = current->getChild();
        }
        return count;
    }

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
    uint64_t nextOriginId = 1;
};

/// Source with two consumers that re-merge (single root): src -> {A, B} -> merge -> sink.
/// The source pipeline must fan out into one distinct pipeline per consumer, and both consumer pipelines
/// must converge on the SAME merge pipeline (fan-in memoization).
TEST_F(PipeliningPhaseFanOutTest, SourceFanOutToTwoConsumers)
{
    auto src = makeSourceWrapper();
    auto consumerA = makeIntermediateWrapper();
    auto consumerB = makeIntermediateWrapper();
    auto merge = makeIntermediateWrapper();
    auto sink = makeSinkWrapper();
    consumerA->addChild(src);
    consumerB->addChild(src);
    merge->addChild(consumerA);
    merge->addChild(consumerB);
    sink->addChild(merge);

    const auto plan = pipeline(sink);

    ASSERT_EQ(plan->getPipelines().size(), 1U);
    const auto sourcePipeline = plan->getPipelines()[0];
    ASSERT_TRUE(sourcePipeline->isSourcePipeline());
    ASSERT_EQ(sourcePipeline->getSuccessors().size(), 2U);
    EXPECT_NE(sourcePipeline->getSuccessors()[0].get(), sourcePipeline->getSuccessors()[1].get());

    std::vector<std::shared_ptr<Pipeline>> mergePipelines;
    for (const auto& branch : sourcePipeline->getSuccessors())
    {
        EXPECT_TRUE(branch->isOperatorPipeline());
        EXPECT_TRUE(branch->getRootOperator().tryGet<ScanPhysicalOperator>());
        EXPECT_EQ(countOperators<UnionPhysicalOperator>(*branch), 1U);
        EXPECT_EQ(countOperators<EmitPhysicalOperator>(*branch), 1U);
        ASSERT_EQ(branch->getSuccessors().size(), 1U);
        mergePipelines.push_back(branch->getSuccessors()[0]);
    }
    EXPECT_EQ(mergePipelines[0].get(), mergePipelines[1].get()) << "both consumers must feed the same merge pipeline";
}

/// A shared operator whose consumers are a (non-native) SINK and an operator branch that reaches the same
/// sink: src -> F -> {sink, A}, A -> sink (a sink may have multiple inputs). The shared pipeline must be
/// closed with exactly one emit; the sink consumer gets its own formatting pipeline (the shared emit must
/// stay native for the sibling consumer), and both paths converge on the SAME sink pipeline.
TEST_F(PipeliningPhaseFanOutTest, FanOutIntoSinkAndOperatorBranch)
{
    auto src = makeSourceWrapper();
    auto sharedF = makeIntermediateWrapper();
    auto consumerA = makeIntermediateWrapper();
    auto sink = makeSinkWrapper();
    sharedF->addChild(src);
    consumerA->addChild(sharedF);
    sink->addChild(sharedF);
    sink->addChild(consumerA);

    const auto plan = pipeline(sink);

    ASSERT_EQ(plan->getPipelines().size(), 1U);
    const auto sourcePipeline = plan->getPipelines()[0];
    ASSERT_EQ(sourcePipeline->getSuccessors().size(), 1U);

    /// The shared pipeline: scan + F + exactly one (native) emit, two distinct successor branches.
    const auto sharedPipeline = sourcePipeline->getSuccessors()[0];
    EXPECT_EQ(countOperators<UnionPhysicalOperator>(*sharedPipeline), 1U);
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*sharedPipeline), 1U);
    ASSERT_EQ(sharedPipeline->getSuccessors().size(), 2U);
    EXPECT_NE(sharedPipeline->getSuccessors()[0].get(), sharedPipeline->getSuccessors()[1].get());

    /// Both branches (the sink's formatting pipeline and the operator branch) must end at the SAME sink
    /// pipeline.
    std::vector<std::shared_ptr<Pipeline>> sinkPipelines;
    for (const auto& branch : sharedPipeline->getSuccessors())
    {
        EXPECT_TRUE(branch->isOperatorPipeline());
        EXPECT_TRUE(branch->getRootOperator().tryGet<ScanPhysicalOperator>());
        EXPECT_EQ(countOperators<EmitPhysicalOperator>(*branch), 1U);
        ASSERT_EQ(branch->getSuccessors().size(), 1U);
        EXPECT_TRUE(branch->getSuccessors()[0]->isSinkPipeline());
        sinkPipelines.push_back(branch->getSuccessors()[0]);
    }
    EXPECT_EQ(sinkPipelines[0].get(), sinkPipelines[1].get()) << "both paths must reach the same sink pipeline";
}

/// Regression: fan-IN (two sources converging on one shared downstream operator) must still produce
/// a single merged pipeline that both source-side pipelines have as their successor.
TEST_F(PipeliningPhaseFanOutTest, FanInMergePointUnchanged)
{
    auto source1 = makeSourceWrapper();
    auto source2 = makeSourceWrapper();
    auto mergeOp = makeIntermediateWrapper();
    auto sink = makeSinkWrapper();
    mergeOp->addChild(source1);
    mergeOp->addChild(source2);
    sink->addChild(mergeOp);

    const auto plan = pipeline(sink);

    ASSERT_EQ(plan->getPipelines().size(), 2U);
    std::vector<std::shared_ptr<Pipeline>> mergedSuccessors;
    for (const auto& sourcePipeline : plan->getPipelines())
    {
        ASSERT_TRUE(sourcePipeline->isSourcePipeline());
        ASSERT_EQ(sourcePipeline->getSuccessors().size(), 1U);
        mergedSuccessors.push_back(sourcePipeline->getSuccessors()[0]);
    }
    /// Both sources must feed the SAME merge pipeline instance.
    EXPECT_EQ(mergedSuccessors[0].get(), mergedSuccessors[1].get());
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*mergedSuccessors[0]), 1U);
}

}
}
