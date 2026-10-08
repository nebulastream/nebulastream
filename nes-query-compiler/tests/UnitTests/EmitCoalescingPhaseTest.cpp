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

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Phases/EmitCoalescingPhase.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <CoalescingEmitOperatorHandler.hpp>
#include <CoalescingEmitPhysicalOperator.hpp>
#include <EmitPhysicalOperator.hpp>
#include <InputFormatterDescriptor.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <PhysicalPlanBuilder.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <QueryId.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourceDescriptorPhysicalOperator.hpp>
#include <UnionPhysicalOperator.hpp>

namespace NES
{
namespace
{
using PipelineLocation = PhysicalOperatorWrapper::PipelineLocation;
constexpr std::chrono::microseconds DELAY{1000};
constexpr uint64_t OPERATOR_BUFFER_SIZE = 4096;

PhysicalOperator closingOperator(const Pipeline& pipeline)
{
    auto current = pipeline.getRootOperator();
    while (const auto child = current.getChild())
    {
        current = *child;
    }
    return current;
}

std::vector<std::shared_ptr<Pipeline>> allPipelines(const PipelinedQueryPlan& plan)
{
    std::vector<std::shared_ptr<Pipeline>> pipelines;
    std::unordered_set<const Pipeline*> visited;
    std::vector<std::shared_ptr<Pipeline>> pending = plan.getPipelines();
    while (!pending.empty())
    {
        auto pipeline = std::move(pending.back());
        pending.pop_back();
        if (visited.insert(pipeline.get()).second)
        {
            pending.insert(pending.end(), pipeline->getSuccessors().begin(), pipeline->getSuccessors().end());
            pipelines.push_back(std::move(pipeline));
        }
    }
    return pipelines;
}
}

class EmitCoalescingPhaseTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("EmitCoalescingPhaseTest.log", LogLevel::LOG_DEBUG); }

    static Schema<UnqualifiedUnboundField, Ordered> createSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
    }

    std::shared_ptr<PhysicalOperatorWrapper> makeSourceWrapper() const
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
            std::move(descriptor.value()), /// NOLINT(bugprone-unchecked-optional-access) checked above
            OriginId(1));
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{sourceOp}, schema, schema, MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::ROW_LAYOUT, PipelineLocation::SCAN);
    }

    std::shared_ptr<PhysicalOperatorWrapper> makeSinkWrapper(const std::string& outputFormat) const
    {
        auto schema = createSchema();
        auto descriptor = sinkCatalog.getAnonymousSink(
            schema, Identifier::parse("Print"), Host("localhost"), {{Identifier::parse("output_format"), outputFormat}}, {});
        EXPECT_TRUE(descriptor.has_value());
        auto sinkOp = SinkPhysicalOperator(descriptor.value()); /// NOLINT(bugprone-unchecked-optional-access) checked above
        return std::make_shared<PhysicalOperatorWrapper>(
            PhysicalOperator{sinkOp},
            schema,
            schema,
            MemoryLayoutType::ROW_LAYOUT,
            MemoryLayoutType::ROW_LAYOUT,
            PipelineLocation::INTERMEDIATE);
    }

    /// A fusible pass-through operator, @see PipeliningPhaseFanOutTest::makeIntermediateWrapper.
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

    static std::shared_ptr<PipelinedQueryPlan> pipeline(const std::shared_ptr<PhysicalOperatorWrapper>& sinkRoot)
    {
        auto builder = PhysicalPlanBuilder(QueryId::createLocal(LocalQueryId(generateUUID())));
        builder.addSinkRoot(sinkRoot);
        builder.setOperatorBufferSize(OPERATOR_BUFFER_SIZE);
        return QueryCompilation::PipeliningPhase::apply(std::move(builder).finalize());
    }

    /// The closing emits of the operator pipelines, each with its id and whether it is a coalescing emit with a coalescing handler.
    static std::vector<std::pair<OperatorId, bool>> emitsOf(const PipelinedQueryPlan& plan)
    {
        std::vector<std::pair<OperatorId, bool>> emits;
        for (const auto& pipeline : allPipelines(plan))
        {
            if (!pipeline->isOperatorPipeline())
            {
                continue;
            }
            const auto closing = closingOperator(*pipeline);
            if (const auto emit = closing.tryGet<EmitPhysicalOperator>())
            {
                emits.emplace_back(emit->id, false);
            }
            else if (const auto coalescing = closing.tryGet<CoalescingEmitPhysicalOperator>())
            {
                const auto& handler = pipeline->getOperatorHandlers().at(coalescing->getOperatorHandlerId());
                emits.emplace_back(coalescing->id, std::dynamic_pointer_cast<CoalescingEmitOperatorHandler>(handler) != nullptr);
            }
        }
        return emits;
    }

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
};

TEST_F(EmitCoalescingPhaseTest, DefaultEmitCoalescesAndKeepsItsId)
{
    auto source = makeSourceWrapper();
    auto op = makeIntermediateWrapper();
    auto sink = makeSinkWrapper("NATIVE");
    op->addChild(source);
    sink->addChild(op);
    const auto plan = pipeline(sink);
    const auto before = emitsOf(*plan);
    ASSERT_EQ(before.size(), 1U);
    EXPECT_FALSE(before[0].second);

    QueryCompilation::EmitCoalescingPhase::apply(*plan, DELAY);

    const auto after = emitsOf(*plan);
    ASSERT_EQ(after.size(), 1U);
    EXPECT_EQ(after[0].first, before[0].first);
    EXPECT_TRUE(after[0].second);
}

TEST_F(EmitCoalescingPhaseTest, FormattingEmitStaysAsItIs)
{
    auto source = makeSourceWrapper();
    auto op = makeIntermediateWrapper();
    auto sink = makeSinkWrapper("CSV");
    op->addChild(source);
    sink->addChild(op);
    const auto plan = pipeline(sink);

    QueryCompilation::EmitCoalescingPhase::apply(*plan, DELAY);

    const auto emits = emitsOf(*plan);
    ASSERT_EQ(emits.size(), 1U);
    EXPECT_FALSE(emits[0].second);
}

/// A shared emit reached through several paths is replaced once, and every consumer's emit coalesces as well.
TEST_F(EmitCoalescingPhaseTest, EveryEmitOfAFanOutCoalesces)
{
    auto source = makeSourceWrapper();
    auto shared = makeIntermediateWrapper();
    auto consumer = makeIntermediateWrapper();
    auto sink = makeSinkWrapper("NATIVE");
    shared->addChild(source);
    consumer->addChild(shared);
    sink->addChild(shared);
    sink->addChild(consumer);
    const auto plan = pipeline(sink);

    QueryCompilation::EmitCoalescingPhase::apply(*plan, DELAY);

    const auto emits = emitsOf(*plan);
    ASSERT_EQ(emits.size(), 2U);
    for (const auto& [id, coalesces] : emits)
    {
        EXPECT_TRUE(coalesces);
    }
}

TEST_F(EmitCoalescingPhaseTest, ZeroDelayLeavesThePlanAsItIs)
{
    auto source = makeSourceWrapper();
    auto op = makeIntermediateWrapper();
    auto sink = makeSinkWrapper("NATIVE");
    op->addChild(source);
    sink->addChild(op);
    const auto plan = pipeline(sink);

    QueryCompilation::EmitCoalescingPhase::apply(*plan, std::chrono::microseconds::zero());

    const auto emits = emitsOf(*plan);
    ASSERT_EQ(emits.size(), 1U);
    EXPECT_FALSE(emits[0].second);
}

}
