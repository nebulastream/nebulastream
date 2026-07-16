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
#include <string>
#include <utility>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
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
        auto descriptor = sourceCatalog.getAnonymousSource(
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

    std::shared_ptr<PhysicalOperatorWrapper> makeSinkWrapper(const std::string& outputFormat = "CSV") const
    {
        auto schema = createSchema();
        auto descriptor = sinkCatalog.getAnonymousSink(
            schema, Identifier::parse("Print"), Host("localhost"), {{Identifier::parse("output_format"), outputFormat}}, {});
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

    /// A generic fusible INTERMEDIATE operator. UnionPhysicalOperator is used purely because it is the cheapest one
    /// to construct — default constructible, no function, handler or state — and is a pass-through at runtime. This
    /// phase never inspects operator semantics or arity, only PipelineLocation and the wrapper's children. A union's
    /// several inputs live in the wrapper DAG, not in this operator (which holds a single child, its downstream link
    /// inside a pipeline), so one input here is not a malformed union, just a no-op operator.
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

    /// An operator that closes its pipeline itself (window/join builds are located this way). Same choice of
    /// UnionPhysicalOperator as in makeIntermediateWrapper, and for the same reason: only the PipelineLocation
    /// differs, which is the sole thing this phase reads off it.
    static std::shared_ptr<PhysicalOperatorWrapper> makeEmitWrapper()
    {
        auto schema = createSchema();
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{UnionPhysicalOperator()},
            schema,
            schema,
            MemoryLayoutType::ROW_LAYOUT,
            MemoryLayoutType::ROW_LAYOUT,
            PipelineLocation::EMIT);
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

/// A shared operator with two consumers that re-merge downstream:
///
///   sink-rooted input:  sink -> merge -> {A, B};  A -> F;  B -> F;  F -> src
///   flipped (pipelining orientation):  src -> F -> {A, B} -> merge -> sink
///
/// F's pipeline must be closed with exactly one emit and each consumer must start its own distinct successor
/// pipeline, both converging on the same merge pipeline:
///
///   [src] -> [scan|F|emit] -+-> [scan|A|emit] -+-> [scan|merge|...] -> ... -> [sink]
///                           +-> [scan|B|emit] -+
///
/// On main, A and B are instead appended sequentially into F's pipeline ([scan|F|A|emit|B|emit]), so at
/// runtime B would consume A's (or the emit's) output instead of F's output, and F's two successor entries
/// are the SAME merge pipeline twice.
TEST_F(PipeliningPhaseFanOutTest, FanOutOperatorGetsOwnPipelinePerConsumer)
{
    auto src = makeSourceWrapper();
    auto sharedF = makeIntermediateWrapper();
    auto consumerA = makeIntermediateWrapper();
    auto consumerB = makeIntermediateWrapper();
    auto merge = makeIntermediateWrapper();
    auto sink = makeSinkWrapper();

    sharedF->addChild(src);
    consumerA->addChild(sharedF);
    consumerB->addChild(sharedF);
    merge->addChild(consumerA);
    merge->addChild(consumerB);
    sink->addChild(merge);

    const auto plan = pipeline(sink);

    ASSERT_EQ(plan->getPipelines().size(), 1U);
    const auto sourcePipeline = plan->getPipelines()[0];
    ASSERT_TRUE(sourcePipeline->isSourcePipeline());
    ASSERT_EQ(sourcePipeline->getSuccessors().size(), 1U);

    /// The pipeline containing the shared operator F must contain ONLY F (plus scan and one emit)...
    const auto sharedPipeline = sourcePipeline->getSuccessors()[0];
    EXPECT_EQ(countOperators<UnionPhysicalOperator>(*sharedPipeline), 1U)
        << "the consumers of the shared operator were fused INTO its pipeline (sequential mis-wiring)";
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*sharedPipeline), 1U)
        << "one emit was appended per consumer instead of closing the pipeline once";

    /// ...and fan out into one DISTINCT pipeline per consumer.
    ASSERT_EQ(sharedPipeline->getSuccessors().size(), 2U);
    const auto branchA = sharedPipeline->getSuccessors()[0];
    const auto branchB = sharedPipeline->getSuccessors()[1];
    EXPECT_NE(branchA.get(), branchB.get())
        << "both successor entries point to the same pipeline (duplicate edge) instead of one pipeline per consumer";

    /// Both consumer branches must converge again rather than each duplicating the merge -> sink subtree.
    ASSERT_EQ(branchA->getSuccessors().size(), 1U);
    ASSERT_EQ(branchB->getSuccessors().size(), 1U);
    EXPECT_EQ(branchA->getSuccessors()[0].get(), branchB->getSuccessors()[0].get()) << "both consumers must feed the same merge pipeline";
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

/// A fan-out point whose consumers are nested: F -> {B, A} with B -> A as well. `A` gets its pipeline while
/// recursing into B and is then revisited straight from F, whose pipeline is already closed. That revisit must
/// not append a second emit to F's pipeline, and both paths must converge on the SAME pipeline for A.
TEST_F(PipeliningPhaseFanOutTest, FanOutRevisitedConsumerAddsNoSecondEmit)
{
    auto src = makeSourceWrapper();
    auto sharedF = makeIntermediateWrapper();
    auto consumerB = makeIntermediateWrapper();
    auto consumerA = makeIntermediateWrapper();
    auto sink = makeSinkWrapper();
    sharedF->addChild(src);
    consumerB->addChild(sharedF);
    consumerA->addChild(consumerB);
    consumerA->addChild(sharedF);
    sink->addChild(consumerB);
    sink->addChild(consumerA);

    const auto plan = pipeline(sink);

    ASSERT_EQ(plan->getPipelines().size(), 1U);
    const auto sourcePipeline = plan->getPipelines()[0];
    ASSERT_EQ(sourcePipeline->getSuccessors().size(), 1U);

    const auto fanOutPipeline = sourcePipeline->getSuccessors()[0];
    EXPECT_EQ(countOperators<UnionPhysicalOperator>(*fanOutPipeline), 1U);
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*fanOutPipeline), 1U) << "revisiting a consumer must not add a second emit";
    ASSERT_EQ(fanOutPipeline->getSuccessors().size(), 2U);

    /// Successors in visit order: the nested consumer B (itself a fan-out point) and the revisited consumer A.
    const auto nestedPipeline = fanOutPipeline->getSuccessors()[0];
    const auto revisitedPipeline = fanOutPipeline->getSuccessors()[1];
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*nestedPipeline), 1U);
    ASSERT_EQ(nestedPipeline->getSuccessors().size(), 2U);
    EXPECT_TRUE(
        nestedPipeline->getSuccessors()[0].get() == revisitedPipeline.get()
        or nestedPipeline->getSuccessors()[1].get() == revisitedPipeline.get())
        << "A must be a single shared pipeline, reached from both F and B";
}

/// Same shape as FanOutIntoSinkAndOperatorBranch, but with a NATIVE sink: the sink consumes the shared
/// (native) fan-out emit directly, so no formatting pipeline may be inserted in between.
TEST_F(PipeliningPhaseFanOutTest, FanOutIntoNativeSinkNeedsNoFormattingPipeline)
{
    auto src = makeSourceWrapper();
    auto sharedF = makeIntermediateWrapper();
    auto consumerA = makeIntermediateWrapper();
    auto sink = makeSinkWrapper("NATIVE");
    sharedF->addChild(src);
    consumerA->addChild(sharedF);
    sink->addChild(sharedF);
    sink->addChild(consumerA);

    const auto plan = pipeline(sink);

    ASSERT_EQ(plan->getPipelines().size(), 1U);
    const auto sourcePipeline = plan->getPipelines()[0];
    ASSERT_EQ(sourcePipeline->getSuccessors().size(), 1U);

    const auto sharedPipeline = sourcePipeline->getSuccessors()[0];
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*sharedPipeline), 1U);
    ASSERT_EQ(sharedPipeline->getSuccessors().size(), 2U);

    /// The sink hangs off the shared pipeline directly; the operator branch reaches the same sink pipeline.
    const auto sinkPipeline = sharedPipeline->getSuccessors()[0];
    const auto branchPipeline = sharedPipeline->getSuccessors()[1];
    EXPECT_TRUE(sinkPipeline->isSinkPipeline()) << "a native sink needs no formatting pipeline in between";
    EXPECT_TRUE(branchPipeline->isOperatorPipeline());
    ASSERT_EQ(branchPipeline->getSuccessors().size(), 1U);
    EXPECT_EQ(branchPipeline->getSuccessors()[0].get(), sinkPipeline.get());
}

/// A fan-out point that reaches a sink which was ALREADY built from another source root:
///   source1 -> G -> sink   and   source2 -> F -> {sink, A}, A -> sink.
/// G builds and memoizes the sink pipeline first, so F reaches it through the memoized path with its own
/// pipeline already closed by a native emit. The non-native sink must still get its own formatting pipeline —
/// the shared emit has to stay native for the sibling consumer A.
TEST_F(PipeliningPhaseFanOutTest, FanOutIntoAlreadyBuiltSinkKeepsFormatting)
{
    auto source1 = makeSourceWrapper();
    auto source2 = makeSourceWrapper();
    auto directG = makeIntermediateWrapper();
    auto sharedF = makeIntermediateWrapper();
    auto consumerA = makeIntermediateWrapper();
    auto sink = makeSinkWrapper();
    directG->addChild(source1);
    sharedF->addChild(source2);
    consumerA->addChild(sharedF);
    sink->addChild(directG);
    sink->addChild(sharedF);
    sink->addChild(consumerA);

    const auto plan = pipeline(sink);

    ASSERT_EQ(plan->getPipelines().size(), 2U);

    /// First root: source1 -> G -> sink. This is what creates and memoizes the sink pipeline.
    const auto firstSourcePipeline = plan->getPipelines()[0];
    ASSERT_EQ(firstSourcePipeline->getSuccessors().size(), 1U);
    const auto directPipeline = firstSourcePipeline->getSuccessors()[0];
    ASSERT_EQ(directPipeline->getSuccessors().size(), 1U);
    const auto sinkPipeline = directPipeline->getSuccessors()[0];
    ASSERT_TRUE(sinkPipeline->isSinkPipeline());

    /// Second root: source2 -> F, whose pipeline fans out with exactly one native emit.
    const auto secondSourcePipeline = plan->getPipelines()[1];
    ASSERT_EQ(secondSourcePipeline->getSuccessors().size(), 1U);
    const auto fanOutPipeline = secondSourcePipeline->getSuccessors()[0];
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*fanOutPipeline), 1U);
    ASSERT_EQ(fanOutPipeline->getSuccessors().size(), 2U);

    /// Successors in visit order: the sink's formatting pipeline and the operator branch. Both end at the
    /// SAME, already-built sink pipeline.
    const auto formattingPipeline = fanOutPipeline->getSuccessors()[0];
    EXPECT_FALSE(formattingPipeline->isSinkPipeline()) << "a non-native sink must not consume the shared native emit directly";
    EXPECT_TRUE(formattingPipeline->getRootOperator().tryGet<ScanPhysicalOperator>());
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*formattingPipeline), 1U);
    ASSERT_EQ(formattingPipeline->getSuccessors().size(), 1U);
    EXPECT_EQ(formattingPipeline->getSuccessors()[0].get(), sinkPipeline.get());

    const auto branchPipeline = fanOutPipeline->getSuccessors()[1];
    ASSERT_EQ(branchPipeline->getSuccessors().size(), 1U);
    EXPECT_EQ(branchPipeline->getSuccessors()[0].get(), sinkPipeline.get());
}

/// A fan-out point with a consumer that closes its own pipeline (PipelineLocation::EMIT): F -> {E, A}.
/// E must start a fresh scan pipeline instead of being appended behind F's fan-out emit.
TEST_F(PipeliningPhaseFanOutTest, FanOutIntoCustomEmitConsumer)
{
    auto src = makeSourceWrapper();
    auto sharedF = makeIntermediateWrapper();
    auto emitConsumer = makeEmitWrapper();
    auto consumerA = makeIntermediateWrapper();
    auto sink = makeSinkWrapper();
    sharedF->addChild(src);
    emitConsumer->addChild(sharedF);
    consumerA->addChild(sharedF);
    sink->addChild(emitConsumer);
    sink->addChild(consumerA);

    const auto plan = pipeline(sink);

    ASSERT_EQ(plan->getPipelines().size(), 1U);
    const auto sourcePipeline = plan->getPipelines()[0];
    ASSERT_EQ(sourcePipeline->getSuccessors().size(), 1U);

    const auto sharedPipeline = sourcePipeline->getSuccessors()[0];
    EXPECT_EQ(countOperators<UnionPhysicalOperator>(*sharedPipeline), 1U) << "the custom emit must not be fused into the shared pipeline";
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*sharedPipeline), 1U);
    ASSERT_EQ(sharedPipeline->getSuccessors().size(), 2U);

    /// Successors in visit order: the custom-emit consumer's own scan pipeline and the fusible consumer's.
    const auto emitPipeline = sharedPipeline->getSuccessors()[0];
    const auto branchPipeline = sharedPipeline->getSuccessors()[1];
    EXPECT_TRUE(emitPipeline->getRootOperator().tryGet<ScanPhysicalOperator>());
    EXPECT_EQ(countOperators<UnionPhysicalOperator>(*emitPipeline), 1U);
    EXPECT_EQ(countOperators<EmitPhysicalOperator>(*emitPipeline), 0U) << "the custom emit closes the pipeline itself";
    ASSERT_EQ(emitPipeline->getSuccessors().size(), 1U);
    ASSERT_EQ(branchPipeline->getSuccessors().size(), 1U);
    EXPECT_EQ(emitPipeline->getSuccessors()[0].get(), branchPipeline->getSuccessors()[0].get())
        << "both consumers must reach the same sink pipeline";
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
