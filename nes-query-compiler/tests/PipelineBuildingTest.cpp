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
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Phases/PipelineBuilder/PipelineBuilder.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>
#include <PhysicalPlan.hpp>
#include <Pipeline.hpp>
#include <QueryOptimizer.hpp>

namespace NES
{
struct PipelineTestParam
{
    std::vector<Transition<State, Event, BuilderContext>> expectedTransitions;
};

static bool checkTransitionsPresent(
    const std::vector<Transition<State, Event, BuilderContext>>& expectedTransitions,
    const std::vector<Transition<State, Event, BuilderContext>>& actualTransitions)
{
    if (expectedTransitions.size() != actualTransitions.size())
    {
        NES_ERROR("Expected {} transitions but found {}", expectedTransitions.size(), actualTransitions.size());
        return false;
    }

    for (size_t i = 0; i < expectedTransitions.size(); ++i)
    {
        const auto& expected = expectedTransitions[i];
        const auto& actual = actualTransitions[i];

        if (expected.from != actual.from || expected.onEvent != actual.onEvent || expected.to != actual.to)
        {
            NES_ERROR(
                "Transition mismatch at position {}:\nExpected: {} --[{}]--> {}\nActual:   {} --[{}]--> {}",
                i,
                magic_enum::enum_name(expected.from),
                magic_enum::enum_name(expected.onEvent),
                magic_enum::enum_name(expected.to),
                magic_enum::enum_name(actual.from),
                magic_enum::enum_name(actual.onEvent),
                magic_enum::enum_name(actual.to));
            return false;
        }
    }
    return true;
}

static void
CheckPipelineTransitions(const PhysicalPlan& physicalPlan, const std::vector<Transition<State, Event, BuilderContext>>& expectedTransitions)
{
    NES_INFO("Building pipelines for physical plan: {}", physicalPlan);
    auto pipelineBuilder = PipelineBuilder();
    auto pipelinedPlan = pipelineBuilder.build(physicalPlan);
    auto actualTransitions = pipelineBuilder.getAllTransitions();
    bool transitionsPresent = checkTransitionsPresent(expectedTransitions, actualTransitions);
    EXPECT_TRUE(transitionsPresent) << "Not all expected transitions were found in the pipeline builder state machine";
    EXPECT_NE(pipelinedPlan, nullptr) << "Pipeline plan should not be null";
    NES_INFO("Total number of configured transitions: {}", actualTransitions.size());
    for (const auto& transition : actualTransitions)
    {
        NES_DEBUG(
            "Transition: {} --[{}]--> {} (Actions: {})",
            magic_enum::enum_name(transition.from),
            magic_enum::enum_name(transition.onEvent),
            magic_enum::enum_name(transition.to),
            transition.actions.size());
    }
}


static PhysicalPlan createSimplePhysicalPlan()
{
    Schema schema(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema = schema.addField("id", DataTypeProvider::provideDataType(DataType::Type::INT64));
    schema = schema.addField("value", DataTypeProvider::provideDataType(DataType::Type::INT64));

    ParserConfig parserConfig;
    parserConfig.parserType = "CSV";
    parserConfig.tupleDelimiter = "|";
    parserConfig.fieldDelimiter = ",";

    auto catalog = SourceCatalog();
    auto source = catalog.addLogicalSource("testSource", schema);

    auto logicalSourceOpt = catalog.addLogicalSource("my_logical_source", *source->getSchema());
    assert(logicalSourceOpt.has_value());
    LogicalSource logicalSource = logicalSourceOpt.value();

    WorkerId workerId{0};
    std::string sourceType = "File";
    int32_t numberOfBuffersInLocalPool = -1;
    DescriptorConfig::Config config = {};

    auto sourceDescriptorOpt
        = catalog.addPhysicalSource(logicalSource, workerId, sourceType, numberOfBuffersInLocalPool, std::move(config), parserConfig);
    assert(sourceDescriptorOpt.has_value());
    SourceDescriptor sourceDescriptor = sourceDescriptorOpt.value();
    auto sourceOp = SourceDescriptorLogicalOperator(sourceDescriptor).withOutputOriginIds({{OriginId(0)}});

    auto sinkDescriptor = std::make_shared<Sinks::SinkDescriptor>("test_sink", DescriptorConfig::Config(), false);
    auto sinkOperator = SinkLogicalOperator();
    sinkOperator.sinkDescriptor = std::move(sinkDescriptor);

    auto logicalPlan = LogicalPlan(sinkOperator.withChildren({sourceOp}));

    QueryExecutionConfiguration conf{};
    QueryOptimizer optimizer(conf);
    return optimizer.optimize(logicalPlan);
}

TEST(PipelineBuildingTest, SimpleSourceSinkPipeline)
{
    auto physicalPlan = createSimplePhysicalPlan();
    std::vector<Transition<State, Event, BuilderContext>> expectedTransitions
        = {{State::BuildingPipeline, Event::EncounterSource, State::BuildingPipeline, {}},
           {State::BuildingPipeline, Event::EncounterCustomEmit, State::AfterEmit, {}},
           {State::BuildingPipeline, Event::DescendChild, State::BuildingPipeline, {}},
           {State::BuildingPipeline, Event::ChildDone, State::BuildingPipeline, {}}};
    CheckPipelineTransitions(physicalPlan, expectedTransitions);
}
}
