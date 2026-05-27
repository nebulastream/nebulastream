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

#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <Util/UppercaseString.hpp>
#include <BaseUnitTest.hpp>
#include <DistributedLogicalPlan.hpp>
#include <QueryId.hpp>

namespace NES
{

using namespace testing;

class DistributedLogicalPlanTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("DistributedLogicalPlanTest.log", LogLevel::LOG_DEBUG); }

protected:
    SourceCatalog sourceCatalog;
    SinkCatalog sinkCatalog;
    int counter = 0;

    LogicalOperator makeSource(const std::string& sourceType, std::optional<std::string> channel = std::nullopt)
    {
        const auto logicalSource = sourceCatalog.addLogicalSource(fmt::format("S{}", ++counter), Schema{}).value();
        std::unordered_map<UppercaseString, std::string> cfg;
        if (channel)
        {
            cfg.emplace(UppercaseString("CHANNEL"), *channel);
            cfg.emplace(UppercaseString("DATA_ENDPOINT"), "somewhere:9090");
        } else
        {
            cfg.emplace(UppercaseString("FILE_PATH"), "i am a file");
        }
        const std::unordered_map<UppercaseString, std::string> parser
            = {{UppercaseString("TYPE"), "CSV"}, {UppercaseString("TUPLE_DELIMITER"), "\n"}, {UppercaseString("FIELD_DELIMITER"), ","}};
        auto descriptor = sourceCatalog.addPhysicalSource(logicalSource, sourceType, Host("localhost"), std::move(cfg), parser).value();
        return LogicalOperator{SourceDescriptorLogicalOperator(std::move(descriptor))};
    }

    SinkLogicalOperator makeSink(const std::string& sinkType, std::optional<std::string> channel = std::nullopt)
    {
        std::unordered_map<UppercaseString, std::string> cfg;
        if (channel)
        {
            cfg.emplace(UppercaseString("CHANNEL"), *channel);
            cfg.emplace(UppercaseString("DATA_ENDPOINT"), "somewhere:9090");
        }
        auto descriptor
            = sinkCatalog.addSinkDescriptor(fmt::format("snk{}", ++counter), Schema{}, sinkType, Host("localhost"), std::move(cfg), {})
                  .value();
        return SinkLogicalOperator(std::move(descriptor));
    }

    static QueryId nextQueryId() { return QueryId::createLocal(LocalQueryId(generateUUID())); }

    LogicalPlan planWith(QueryId id, SinkLogicalOperator sink, LogicalOperator source)
    {
        return LogicalPlan(std::move(id), {LogicalOperator{sink.withChildren({std::move(source)})}});
    }

    LogicalPlan planWith(QueryId id, SinkLogicalOperator sink, std::vector<LogicalOperator> sources)
    {
        return LogicalPlan(std::move(id), {LogicalOperator{sink.withChildren(std::move(sources))}});
    }

    /// Project the topologicalSort output down to just the QueryIds in each wave.
    /// Order within a wave is unspecified (the algorithm iterates an unordered_map),
    /// so callers compare using UnorderedElementsAre at the inner level.
    static std::vector<std::vector<QueryId>> idsOf(const std::vector<std::vector<std::pair<Host, LogicalPlan>>>& waves)
    {
        std::vector<std::vector<QueryId>> ids;
        ids.reserve(waves.size());
        for (const auto& wave : waves)
        {
            std::vector<QueryId> waveIds;
            waveIds.reserve(wave.size());
            for (const auto& [_, plan] : wave)
            {
                waveIds.push_back(plan.getQueryId());
            }
            ids.push_back(std::move(waveIds));
        }
        return ids;
    }

    static std::vector<std::vector<std::pair<Host, LogicalPlan>>> drain(std::unordered_map<Host, std::vector<LogicalPlan>> plans)
    {
        std::vector<std::vector<std::pair<Host, LogicalPlan>>> waves;
        for (auto&& wave : topologicalSort(std::move(plans)))
        {
            waves.push_back(std::move(wave));
        }
        return waves;
    }
};

/// A plan with no network sources at all should be yielded immediately in a single wave.
TEST_F(DistributedLogicalPlanTest, AllReadyImmediately)
{
    const Host h{"worker-1:8080"};
    const auto id = nextQueryId();
    const auto plan = planWith(id, makeSink("Print"), makeSource("File"));

    const auto waves = drain({{h, {plan}}});

    EXPECT_THAT(idsOf(waves), ElementsAre(UnorderedElementsAre(id)));
}

/// Producer (File -> NETWORK sink on channel "c1") must be yielded before
/// consumer (NETWORK source on "c1" -> Print sink).
///
/// EXPECTED TO FAIL against the current implementation: the sink filter in
/// topologicalSort is `!= "NETWORK"`, so the producer's channel is never added
/// to `usedChannels` and the consumer never becomes ready — the generator hangs
/// emitting empty waves. Flipping the filter to `== "NETWORK"` makes this pass.
TEST_F(DistributedLogicalPlanTest, ProducerBeforeConsumerAcrossNetworkChannel)
{
    const Host producerHost{"worker-1:8080"};
    const Host consumerHost{"worker-2:8080"};

    const auto producerId = nextQueryId();
    const auto consumerId = nextQueryId();
    const auto producerPlan = planWith(producerId, makeSink("NETWORK", "c1"), makeSource("File"));
    const auto consumerPlan = planWith(consumerId, makeSink("Print"), makeSource("NETWORK", "c1"));

    const auto waves = drain({{producerHost, {producerPlan}}, {consumerHost, {consumerPlan}}});

    EXPECT_THAT(
        idsOf(waves),
        ElementsAre(UnorderedElementsAre(producerId), UnorderedElementsAre(consumerId)));
}

/// Three-stage linear pipeline through a mixed intermediate node:
///   producer:    File          -> NETWORK sink (c1)
///   intermediate: File + NETWORK("c1") -> NETWORK sink (c2)   <-- mixed sources
///   consumer:    NETWORK("c2") -> Print
/// Expected ordering: producer, intermediate, consumer.
TEST_F(DistributedLogicalPlanTest, MixedSourcesIntermediateOrdersAfterAllNetworkInputsReady)
{
    const Host producerHost{"worker-1:8080"};
    const Host intermediateHost{"worker-2:8080"};
    const Host consumerHost{"worker-3:8080"};

    const auto producerId = nextQueryId();
    const auto intermediateId = nextQueryId();
    const auto consumerId = nextQueryId();

    const auto producerPlan = planWith(producerId, makeSink("NETWORK", "c1"), makeSource("File"));
    const auto intermediatePlan
        = planWith(intermediateId, makeSink("NETWORK", "c2"), {makeSource("File"), makeSource("NETWORK", "c1")});
    const auto consumerPlan = planWith(consumerId, makeSink("Print"), makeSource("NETWORK", "c2"));

    const auto waves
        = drain({{producerHost, {producerPlan}}, {intermediateHost, {intermediatePlan}}, {consumerHost, {consumerPlan}}});

    EXPECT_THAT(
        idsOf(waves),
        ElementsAre(
            UnorderedElementsAre(producerId),
            UnorderedElementsAre(intermediateId),
            UnorderedElementsAre(consumerId)));
}

/// An intermediate that depends on two upstream channels must wait for *both*
/// of them before being yielded — not just one.
///   producer-a:  File          -> NETWORK sink (a)
///   producer-b:  File          -> NETWORK sink (b)
///   joiner:      NETWORK(a) + NETWORK(b) -> Print
/// Expected: producer-a and producer-b come out together in wave 0; joiner in wave 1.
TEST_F(DistributedLogicalPlanTest, IntermediateWaitsForAllNetworkInputs)
{
    const Host producerA{"worker-a:8080"};
    const Host producerB{"worker-b:8080"};
    const Host joiner{"worker-join:8080"};

    const auto idA = nextQueryId();
    const auto idB = nextQueryId();
    const auto idJoin = nextQueryId();

    const auto planA = planWith(idA, makeSink("NETWORK", "a"), makeSource("File"));
    const auto planB = planWith(idB, makeSink("NETWORK", "b"), makeSource("File"));
    const auto joinPlan = planWith(idJoin, makeSink("Print"), {makeSource("NETWORK", "a"), makeSource("NETWORK", "b")});

    const auto waves = drain({{producerA, {planA}}, {producerB, {planB}}, {joiner, {joinPlan}}});

    EXPECT_THAT(
        idsOf(waves),
        ElementsAre(UnorderedElementsAre(idA, idB), UnorderedElementsAre(idJoin)));
}

/// Diamond: one source fans out to two intermediates that each forward over a
/// distinct channel, and a final consumer joins both back together.
///   root:    File          -> NETWORK sink (root-out)
///   left:    NETWORK(root-out) -> NETWORK sink (left)
///   right:   NETWORK(root-out) -> NETWORK sink (right)
///   sink:    NETWORK(left) + NETWORK(right) -> Print
/// Expected waves: {root}, {left, right}, {sink}.
TEST_F(DistributedLogicalPlanTest, DiamondTopology)
{
    const Host rootHost{"worker-root:8080"};
    const Host leftHost{"worker-left:8080"};
    const Host rightHost{"worker-right:8080"};
    const Host sinkHost{"worker-sink:8080"};

    const auto rootId = nextQueryId();
    const auto leftId = nextQueryId();
    const auto rightId = nextQueryId();
    const auto sinkId = nextQueryId();

    const auto rootPlan = planWith(rootId, makeSink("NETWORK", "root-out"), makeSource("File"));
    const auto leftPlan = planWith(leftId, makeSink("NETWORK", "left"), makeSource("NETWORK", "root-out"));
    const auto rightPlan = planWith(rightId, makeSink("NETWORK", "right"), makeSource("NETWORK", "root-out"));
    const auto sinkPlan
        = planWith(sinkId, makeSink("Print"), {makeSource("NETWORK", "left"), makeSource("NETWORK", "right")});

    const auto waves
        = drain({{rootHost, {rootPlan}}, {leftHost, {leftPlan}}, {rightHost, {rightPlan}}, {sinkHost, {sinkPlan}}});

    EXPECT_THAT(
        idsOf(waves),
        ElementsAre(
            UnorderedElementsAre(rootId),
            UnorderedElementsAre(leftId, rightId),
            UnorderedElementsAre(sinkId)));
}

/// A single host may carry several plans at different pipeline depths — each one
/// should still be ordered by its own data dependencies, not grouped by host.
TEST_F(DistributedLogicalPlanTest, MultiplePlansPerHostOrderedIndependently)
{
    const Host hostA{"worker-a:8080"};
    const Host hostB{"worker-b:8080"};

    const auto producerId = nextQueryId();
    const auto relayId = nextQueryId();
    const auto finalId = nextQueryId();

    /// On hostA: a producer (File -> NETWORK c1) and a sink (NETWORK c2 -> Print).
    /// On hostB: a relay that bridges them (NETWORK c1 -> NETWORK c2).
    const auto producer = planWith(producerId, makeSink("NETWORK", "c1"), makeSource("File"));
    const auto finalSink = planWith(finalId, makeSink("Print"), makeSource("NETWORK", "c2"));
    const auto relay = planWith(relayId, makeSink("NETWORK", "c2"), makeSource("NETWORK", "c1"));

    const auto waves = drain({{hostA, {producer, finalSink}}, {hostB, {relay}}});

    EXPECT_THAT(
        idsOf(waves),
        ElementsAre(
            UnorderedElementsAre(producerId),
            UnorderedElementsAre(relayId),
            UnorderedElementsAre(finalId)));
}

}
