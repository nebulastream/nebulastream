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

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DistributedLogicalPlan.hpp>
#include <DistributedTestCluster.hpp>

namespace NES
{

using namespace ::testing;

class QueryDecompositionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("QueryDecompositionTest.log", LogLevel::LOG_DEBUG); }
};

/// A selection on the leaf sends its output over a base station to the sink in the cloud. The decomposition records one channel per hop,
/// both with the selection as upstream and the sink as downstream logical operator, and the operators each local plan hosts.
TEST_F(QueryDecompositionTest, RecordsOneChannelPerHop)
{
    DistributedTestCluster cluster;
    cluster.addWorker("cloud:1", {});
    cluster.addWorker("base:1", {"cloud:1"});
    cluster.addWorker("leaf:1", {"base:1"});
    cluster.addSource("src", {"a", "b"}, "leaf:1");
    cluster.addSink("snk", {"a", "b"}, "cloud:1");
    const auto plan = cluster.optimize("SELECT * FROM src WHERE a > b INTO snk");

    const auto selection = getOperatorByType<SelectionLogicalOperator>(plan.getGlobalPlan());
    const auto sink = getOperatorByType<SinkLogicalOperator>(plan.getGlobalPlan());
    ASSERT_THAT(selection, SizeIs(1));
    ASSERT_THAT(sink, SizeIs(1));
    const auto source = getOperatorByType<SourceDescriptorLogicalOperator>(plan.getGlobalPlan());
    ASSERT_THAT(source, SizeIs(1));

    EXPECT_THAT(
        plan.getHostedOperators(Host("leaf:1")), ElementsAre(UnorderedElementsAre(selection.front().getId(), source.front().getId())));
    EXPECT_THAT(plan.getHostedOperators(Host("base:1")), ElementsAre(IsEmpty()));
    EXPECT_THAT(plan.getHostedOperators(Host("cloud:1")), ElementsAre(ElementsAre(sink.front().getId())));

    const auto& channels = plan.getChannels();
    ASSERT_THAT(channels, SizeIs(2));
    EXPECT_THAT(channels.at(0).upstreamWorker, Eq(Host("leaf:1")));
    EXPECT_THAT(channels.at(0).downstreamWorker, Eq(Host("base:1")));
    EXPECT_THAT(channels.at(1).upstreamWorker, Eq(Host("base:1")));
    EXPECT_THAT(channels.at(1).downstreamWorker, Eq(Host("cloud:1")));
    EXPECT_THAT(channels.at(0).id, Ne(channels.at(1).id));
    for (const auto& channel : channels)
    {
        EXPECT_THAT(channel.upstreamOperatorId, Eq(selection.front().getId()));
        EXPECT_THAT(channel.downstreamOperatorId, Eq(sink.front().getId()));
        EXPECT_THAT(channel.upstreamPlanIndex, Eq(0U));
        EXPECT_THAT(channel.downstreamPlanIndex, Eq(0U));
    }
}

/// The local plan indices of a channel point to the plans holding its network sink and source, also with several plans per worker.
TEST_F(QueryDecompositionTest, ChannelsPointToTheirLocalPlans)
{
    DistributedTestCluster cluster;
    cluster.addWorker("cloud:1", {});
    cluster.addWorker("base:1", {"cloud:1"});
    cluster.addWorker("leaf1:1", {"base:1"});
    cluster.addWorker("leaf2:1", {"base:1"});
    cluster.addSource("src1", {"a", "b"}, "leaf1:1");
    cluster.addSource("src2", {"a", "b"}, "leaf2:1");
    cluster.addSink("snk", {"a", "b"}, "cloud:1");
    const auto plan = cluster.optimize("SELECT * FROM src1 UNION SELECT * FROM src2 INTO snk");

    for (const auto& channel : plan.getChannels())
    {
        ASSERT_THAT(channel.upstreamPlanIndex, Lt(plan[channel.upstreamWorker].size()));
        ASSERT_THAT(channel.downstreamPlanIndex, Lt(plan[channel.downstreamWorker].size()));
    }
    /// Every local plan except the roots sends over exactly one channel, every plan with a network source receives over one.
    size_t numberOfPlans = 0;
    for (const auto& [worker, plans] : plan)
    {
        numberOfPlans += plans.size();
        for (size_t index = 0; index < plans.size(); ++index)
        {
            const auto sending = std::ranges::count_if(
                plan.getChannels(),
                [&](const auto& channel) { return channel.upstreamWorker == worker && channel.upstreamPlanIndex == index; });
            EXPECT_THAT(sending, Le(1)) << worker << " plan " << index;
        }
    }
    EXPECT_THAT(plan.getChannels().size(), Eq(numberOfPlans - 1));
}

}
