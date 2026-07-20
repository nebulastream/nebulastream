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

/// Verifies the fix for "Instantiate query plans with multiple sinks": ExecutableQueryPlan::instantiate
/// previously threw NotImplemented for more than one sink. It now creates and wires one sink pipeline per
/// sink; every sink owns one controller of a shared counting backpressure channel, so any slow sink
/// throttles the sources (see BackpressureChannelTest for the channel semantics).

#include <algorithm>
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
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceProvider.hpp>
#include <CompiledQueryPlan.hpp>
#include <ExecutableQueryPlan.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace
{

class MultiSinkInstantiateTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("MultiSinkInstantiateTest.log", LogLevel::LOG_DEBUG); }

    static Schema<UnqualifiedUnboundField, Ordered> createSchema()
    {
        return Schema<UnqualifiedUnboundField, Ordered>{
            {Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
            {Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
    }

    SourceDescriptor makeSourceDescriptor()
    {
        auto descriptor = sourceCatalog.getInlineSource(
            Identifier::parse("File"),
            createSchema(),
            Host("localhost"),
            {{Identifier::parse(InputFormatterDescriptor::getTypeString()), "CSV"}},
            {{Identifier::parse("file_path"), "/dev/null"}});
        EXPECT_TRUE(descriptor.has_value());
        return descriptor.value();
    }

    SinkDescriptor makeSinkDescriptor()
    {
        auto descriptor = sinkCatalog.getInlineSink(
            createSchema(), Identifier::parse("Print"), Host("localhost"), {{Identifier::parse("output_format"), "CSV"}}, {});
        EXPECT_TRUE(descriptor.has_value());
        return descriptor.value();
    }

    /// Builds a CompiledQueryPlan with one source and sinkCount sinks, each sink a direct successor of
    /// the source (the wiring instantiate is responsible for).
    std::unique_ptr<CompiledQueryPlan> makePlan(const size_t sinkCount)
    {
        const auto sourceOperatorId = OperatorId(42);
        std::vector<CompiledQueryPlan::Sink> sinks;
        sinks.reserve(sinkCount);
        for (size_t i = 0; i < sinkCount; ++i)
        {
            sinks.push_back(
                CompiledQueryPlan::Sink{.id = PipelineId(i + 1), .descriptor = makeSinkDescriptor(), .predecessor = {sourceOperatorId}});
        }
        std::vector<CompiledQueryPlan::Source> sources;
        sources.push_back(CompiledQueryPlan::Source{
            .originId = OriginId(1), .operatorId = sourceOperatorId, .descriptor = makeSourceDescriptor(), .successors = {}});
        return CompiledQueryPlan::create(QueryId::createLocal(LocalQueryId(generateUUID())), {}, std::move(sinks), std::move(sources));
    }

    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
};

/// One source, two sinks: instantiate must create one sink pipeline per sink and register BOTH as
/// successors of the source. On main this threw NotImplemented.
TEST_F(MultiSinkInstantiateTest, TwoSinksAreInstantiatedAndWired)
{
    const auto plan = makePlan(2);
    const auto sourceProvider = SourceProvider{1, BufferManager::create()};

    const auto executable = ExecutableQueryPlan::instantiate(*plan, sourceProvider);

    ASSERT_TRUE(executable);
    EXPECT_EQ(executable->pipelines.size(), 2U) << "one sink pipeline per sink must be created";
    ASSERT_EQ(executable->sources.size(), 1U);

    const auto& [source, successors] = executable->sources[0];
    ASSERT_EQ(successors.size(), 2U) << "both sinks must be wired as successors of the source";
    std::vector<PipelineId> successorIds;
    for (const auto& successor : successors)
    {
        successorIds.push_back(successor.lock()->id);
    }
    std::ranges::sort(successorIds);
    EXPECT_EQ(successorIds, (std::vector{PipelineId(1), PipelineId(2)}));
}

/// Regression: the single-sink path behaves as before.
TEST_F(MultiSinkInstantiateTest, SingleSinkStillInstantiates)
{
    const auto plan = makePlan(1);
    const auto sourceProvider = SourceProvider{1, BufferManager::create()};

    const auto executable = ExecutableQueryPlan::instantiate(*plan, sourceProvider);

    ASSERT_TRUE(executable);
    EXPECT_EQ(executable->pipelines.size(), 1U);
    ASSERT_EQ(executable->sources.size(), 1U);
    EXPECT_EQ(executable->sources[0].second.size(), 1U);
}

}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
