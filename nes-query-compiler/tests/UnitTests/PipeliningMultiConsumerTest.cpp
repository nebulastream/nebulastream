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

/// Verifies the fix for "Split pipelines at operators with multiple consumers in the pipelining phase":
/// a shared operator's pipeline must be closed with exactly one emit and fan out into one distinct
/// successor pipeline per consumer. Without the fix, the consumers were appended sequentially into ONE
/// linear pipeline (the second consumer would read the first consumer's output at runtime), with one emit
/// stacked per consumer. The physical plan is hand-built because lowering multi-parent logical plans is a
/// separate issue; once that lands, an end-to-end variant through the QueryCompiler becomes possible.

#include <cstddef>
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

class PipeliningMultiConsumerTest : public Testing::BaseUnitTest
{
public:
    static constexpr uint64_t BUFFER_SIZE = 4096;

    static void SetUpTestSuite() { Logger::setupLogging("PipeliningMultiConsumerTest.log", LogLevel::LOG_DEBUG); }

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
            OriginId(1));
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
        /// Real sink lowering (LowerToPhysicalSink) places sinks as INTERMEDIATE.
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
};

/// Single-root diamond so that the (still single-root) flip accepts it:
///
///   sink-rooted input:  sink -> merge -> {A, B};  A -> F;  B -> F;  F -> src
///   flipped (pipelining orientation):  src -> F -> {A, B} -> merge -> sink
///
/// F is a shared operator with TWO consumers. Expected pipeline shape: F's pipeline is closed with exactly
/// one emit and each consumer starts its own (distinct) successor pipeline:
///
///   [src] -> [scan|F|emit] -+-> [scan|A|emit] -+-> [scan|merge|...] -> ... -> [sink]
///                           +-> [scan|B|emit] -+
///
/// On main, A and B are instead appended sequentially into F's pipeline ([scan|F|A|emit|B|emit]), so at
/// runtime B would consume A's (or the emit's) output instead of F's output, and F's two successor entries
/// are the SAME merge pipeline twice.
TEST_F(PipeliningMultiConsumerTest, FanOutOperatorGetsOwnPipelinePerConsumer)
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

    auto builder = PhysicalPlanBuilder(randomQueryId());
    builder.addSinkRoot(sink);
    builder.setOperatorBufferSize(BUFFER_SIZE);
    const auto pipelined = QueryCompilation::PipeliningPhase::apply(std::move(builder).finalize());

    ASSERT_EQ(pipelined->getPipelines().size(), 1U);
    const auto sourcePipeline = pipelined->getPipelines()[0];
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
    EXPECT_NE(sharedPipeline->getSuccessors()[0].get(), sharedPipeline->getSuccessors()[1].get())
        << "both successor entries point to the same pipeline (duplicate edge) instead of one pipeline per consumer";
}

}
}
