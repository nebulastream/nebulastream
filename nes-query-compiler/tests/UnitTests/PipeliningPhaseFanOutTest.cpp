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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <EmitPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <PhysicalPlanBuilder.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>
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

    static Schema createSchema()
    {
        Schema schema;
        schema.addField("test.id", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        schema.addField("test.value", DataTypeProvider::provideDataType(DataType::Type::UINT64));
        return schema;
    }

    std::shared_ptr<PhysicalOperatorWrapper> makeSourceWrapper()
    {
        auto schema = createSchema();
        auto descriptor = sourceCatalog.getInlineSource("File", schema, Host("localhost"), {{"type", "CSV"}}, {{"file_path", "/dev/null"}});
        EXPECT_TRUE(descriptor.has_value());
        auto sourceOp = SourcePhysicalOperator(
            std::move(descriptor.value()), /// NOLINT(bugprone-unchecked-optional-access)
            OriginId(nextOriginId++));
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{sourceOp}, schema, schema, MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::ROW_LAYOUT, PipelineLocation::SCAN);
    }

    std::shared_ptr<PhysicalOperatorWrapper> makeSinkWrapper() const
    {
        auto schema = createSchema();
        auto descriptor = sinkCatalog.getInlineSink(schema, "Print", Host("localhost"), {{"output_format", "CSV"}}, {});
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
        auto unionOp = UnionPhysicalOperator();
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{unionOp},
            schema,
            schema,
            MemoryLayoutType::ROW_LAYOUT,
            MemoryLayoutType::ROW_LAYOUT,
            PipelineLocation::INTERMEDIATE);
    }

    /// Builds the PhysicalPlan for the given sink-rooted wrappers and runs the pipelining phase.
    static std::shared_ptr<PipelinedQueryPlan>
    pipeline(const std::vector<std::shared_ptr<PhysicalOperatorWrapper>>& sinkRoots, const bool tappablePipelines = false)
    {
        auto builder = PhysicalPlanBuilder(randomQueryId());
        for (const auto& sinkRoot : sinkRoots)
        {
            builder.addSinkRoot(sinkRoot);
        }
        builder.setOperatorBufferSize(BUFFER_SIZE);
        return QueryCompilation::PipeliningPhase::apply(std::move(builder).finalize(), tappablePipelines);
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

/// Source fans out directly to two sinks. Each sink gets its own formatting pipeline (CSV sinks),
/// and the source pipeline has both branches as successors.
TEST_F(PipeliningPhaseFanOutTest, SourceFanOutToTwoSinks)
{
    auto source = makeSourceWrapper();
    auto sinkA = makeSinkWrapper();
    auto sinkB = makeSinkWrapper();
    sinkA->addChild(source);
    sinkB->addChild(source);

    const auto plan = pipeline({sinkA, sinkB});

    ASSERT_EQ(plan->getPipelines().size(), 1U);
    const auto sourcePipeline = plan->getPipelines()[0];
    ASSERT_TRUE(sourcePipeline->isSourcePipeline());
    ASSERT_EQ(sourcePipeline->getSuccessors().size(), 2U);

    for (const auto& branch : sourcePipeline->getSuccessors())
    {
        EXPECT_TRUE(branch->isOperatorPipeline());
        EXPECT_TRUE(branch->getRootOperator().tryGet<ScanPhysicalOperator>());
        EXPECT_EQ(countOperators<EmitPhysicalOperator>(*branch), 1U);
        ASSERT_EQ(branch->getSuccessors().size(), 1U);
        EXPECT_TRUE(branch->getSuccessors()[0]->isSinkPipeline());
    }
}

/// A shared intermediate operator fans out to two sinks: the shared pipeline must be closed with
/// exactly ONE emit and have one successor branch per sink (formatting pipeline + sink pipeline).
TEST_F(PipeliningPhaseFanOutTest, OperatorFanOutToTwoSinks)
{
    auto source = makeSourceWrapper();
    auto shared = makeIntermediateWrapper();
    auto sinkA = makeSinkWrapper();
    auto sinkB = makeSinkWrapper();
    shared->addChild(source);
    sinkA->addChild(shared);
    sinkB->addChild(shared);

    const auto plan = pipeline({sinkA, sinkB});

    ASSERT_EQ(plan->getPipelines().size(), 1U);
    const auto sourcePipeline = plan->getPipelines()[0];
    ASSERT_EQ(sourcePipeline->getSuccessors().size(), 1U);

    /// The shared operator pipeline: scan + union + exactly one emit, fanning out to two branches.
    const auto sharedPipeline = sourcePipeline->getSuccessors()[0];
    ASSERT_TRUE(sharedPipeline->isOperatorPipeline());
    EXPECT_TRUE(sharedPipeline->getRootOperator().tryGet<ScanPhysicalOperator>());
    EXPECT_EQ(countOperators<UnionPhysicalOperator>(*sharedPipeline), 1U);
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*sharedPipeline), 1U);
    ASSERT_EQ(sharedPipeline->getSuccessors().size(), 2U);

    for (const auto& branch : sharedPipeline->getSuccessors())
    {
        /// CSV sinks need a formatting pipeline per branch; the shared emit stays native.
        EXPECT_TRUE(branch->isOperatorPipeline());
        EXPECT_TRUE(branch->getRootOperator().tryGet<ScanPhysicalOperator>());
        EXPECT_EQ(countOperators<EmitPhysicalOperator>(*branch), 1U);
        ASSERT_EQ(branch->getSuccessors().size(), 1U);
        EXPECT_TRUE(branch->getSuccessors()[0]->isSinkPipeline());
    }
}

/// A shared intermediate operator fans out into two operator branches (each with a further
/// intermediate operator before its sink). The consumer pipelines must each start with a scan and
/// must not duplicate the shared pipeline's emit.
TEST_F(PipeliningPhaseFanOutTest, OperatorFanOutToOperatorBranches)
{
    auto source = makeSourceWrapper();
    auto shared = makeIntermediateWrapper();
    auto branchOpA = makeIntermediateWrapper();
    auto branchOpB = makeIntermediateWrapper();
    auto sinkA = makeSinkWrapper();
    auto sinkB = makeSinkWrapper();
    shared->addChild(source);
    branchOpA->addChild(shared);
    branchOpB->addChild(shared);
    sinkA->addChild(branchOpA);
    sinkB->addChild(branchOpB);

    const auto plan = pipeline({sinkA, sinkB});

    ASSERT_EQ(plan->getPipelines().size(), 1U);
    const auto sourcePipeline = plan->getPipelines()[0];
    ASSERT_EQ(sourcePipeline->getSuccessors().size(), 1U);

    const auto sharedPipeline = sourcePipeline->getSuccessors()[0];
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*sharedPipeline), 1U);
    ASSERT_EQ(sharedPipeline->getSuccessors().size(), 2U);

    for (const auto& branch : sharedPipeline->getSuccessors())
    {
        EXPECT_TRUE(branch->isOperatorPipeline());
        EXPECT_TRUE(branch->getRootOperator().tryGet<ScanPhysicalOperator>());
        EXPECT_EQ(countOperators<UnionPhysicalOperator>(*branch), 1U);
        EXPECT_EQ(countOperators<EmitPhysicalOperator>(*branch), 1U);
    }
}

/// Without tappable compilation, a single non-native sink fuses its formatting emit INTO the operator
/// pipeline: the operator pipeline's boundary carries formatted bytes and is NOT a valid tap point.
/// With tappable compilation, the operator pipeline is closed with a native emit and the sink formats in
/// its own pipeline (exactly the shape fan-out points produce).
TEST_F(PipeliningPhaseFanOutTest, TappableModeSplitsFormattingSinkFromOperatorPipeline)
{
    auto makeShape = [&](const bool tappable)
    {
        auto source = makeSourceWrapper();
        auto op = makeIntermediateWrapper();
        auto sink = makeSinkWrapper();
        op->addChild(source);
        sink->addChild(op);
        return pipeline({sink}, tappable);
    };

    /// Default: source -> [scan|union|fmtEmit] -> [sink]: the operator pipeline's successor IS the sink.
    const auto fusedPlan = makeShape(false);
    ASSERT_EQ(fusedPlan->getPipelines().size(), 1U);
    const auto fusedOperatorPipeline = fusedPlan->getPipelines()[0]->getSuccessors().at(0);
    ASSERT_EQ(fusedOperatorPipeline->getSuccessors().size(), 1U);
    EXPECT_TRUE(fusedOperatorPipeline->getSuccessors()[0]->isSinkPipeline());

    /// Tappable: source -> [scan|union|nativeEmit] -> [scan|fmtEmit] -> [sink]: the operator pipeline's
    /// successor is a formatting pipeline, so its boundary stays native (= tappable).
    const auto tappablePlan = makeShape(true);
    ASSERT_EQ(tappablePlan->getPipelines().size(), 1U);
    const auto operatorPipeline = tappablePlan->getPipelines()[0]->getSuccessors().at(0);
    EXPECT_EQ(countOperators<UnionPhysicalOperator>(*operatorPipeline), 1U);
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*operatorPipeline), 1U);
    ASSERT_EQ(operatorPipeline->getSuccessors().size(), 1U);
    const auto formattingPipeline = operatorPipeline->getSuccessors()[0];
    EXPECT_TRUE(formattingPipeline->isOperatorPipeline());
    EXPECT_TRUE(formattingPipeline->getRootOperator().tryGet<ScanPhysicalOperator>());
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*formattingPipeline), 1U);
    ASSERT_EQ(formattingPipeline->getSuccessors().size(), 1U);
    EXPECT_TRUE(formattingPipeline->getSuccessors()[0]->isSinkPipeline());
}

/// Tappable compilation also splits the parsing pipeline of a source->sink passthrough query, keeping the
/// parsed (native) data observable between parsing and output formatting.
TEST_F(PipeliningPhaseFanOutTest, TappableModeSplitsPassthroughFormatting)
{
    auto makeShape = [&](const bool tappable)
    {
        auto source = makeSourceWrapper();
        auto sink = makeSinkWrapper();
        sink->addChild(source);
        return pipeline({sink}, tappable);
    };

    /// Default: source -> [scan(parse)|fmtEmit] -> [sink].
    const auto fusedPlan = makeShape(false);
    const auto fusedParsingPipeline = fusedPlan->getPipelines()[0]->getSuccessors().at(0);
    ASSERT_EQ(fusedParsingPipeline->getSuccessors().size(), 1U);
    EXPECT_TRUE(fusedParsingPipeline->getSuccessors()[0]->isSinkPipeline());

    /// Tappable: source -> [scan(parse)|nativeEmit] -> [scan|fmtEmit] -> [sink].
    const auto tappablePlan = makeShape(true);
    const auto parsingPipeline = tappablePlan->getPipelines()[0]->getSuccessors().at(0);
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*parsingPipeline), 1U);
    ASSERT_EQ(parsingPipeline->getSuccessors().size(), 1U);
    const auto formattingPipeline = parsingPipeline->getSuccessors()[0];
    EXPECT_TRUE(formattingPipeline->isOperatorPipeline());
    ASSERT_EQ(formattingPipeline->getSuccessors().size(), 1U);
    EXPECT_TRUE(formattingPipeline->getSuccessors()[0]->isSinkPipeline());
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

    const auto plan = pipeline({sink});

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
