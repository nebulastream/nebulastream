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

#include <NetworkTopology.hpp>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

using namespace ::testing;

class NetworkTopologyTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TopologyTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down TopologyTest class."); }
};

TEST_F(NetworkTopologyTest, SingleNodeTopology)
{
    const auto node = WorkerId("host");
    auto topology = NetworkTopology{};
    topology.addNode(node, {});
    EXPECT_THAT(topology, SizeIs(1));
    EXPECT_THAT(topology.getDownstreamNodesOf(node), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node), IsEmpty());
}

TEST_F(NetworkTopologyTest, TwoNodesWithoutUpOrDownstream)
{
    const auto node1 = WorkerId("node1");
    const auto node2 = WorkerId("node2");
    auto topology = NetworkTopology{};
    topology.addNode(node1, {});
    topology.addNode(node2, {});
    EXPECT_THAT(topology.getDownstreamNodesOf(node1), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node1), IsEmpty());
    EXPECT_THAT(topology.getDownstreamNodesOf(node2), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node2), IsEmpty());
}

TEST_F(NetworkTopologyTest, TwoNodesConnection)
{
    const auto node1 = WorkerId("node1");
    const auto node2 = WorkerId("node2");
    auto topology = NetworkTopology{};
    topology.addNode(node1, {node2});
    topology.addNode(node2, {});
    EXPECT_THAT(topology.getUpstreamNodesOf(node1), IsEmpty());
    EXPECT_THAT(topology.getDownstreamNodesOf(node1), Contains(node2));
    EXPECT_THAT(topology.getUpstreamNodesOf(node2), Contains(node1));
    EXPECT_THAT(topology.getDownstreamNodesOf(node2), IsEmpty());
}

/// Paths
TEST_F(NetworkTopologyTest, FindPathSimpleTwoNodes)
{
    const auto node1 = WorkerId("node1");
    const auto node2 = WorkerId("node2");
    auto topology = NetworkTopology{};
    topology.addNode(node1, {node2});
    topology.addNode(node2, {});
    EXPECT_THAT(topology.findPaths(node1, node2, NetworkTopology::Downstream), Contains(NetworkTopology::Path{{node1, node2}}));
    EXPECT_THAT(topology.findPaths(node2, node1, NetworkTopology::Downstream), IsEmpty());

    EXPECT_THAT(topology.findPaths(node2, node1, NetworkTopology::Upstream), Contains(NetworkTopology::Path{{node2, node1}}));
    EXPECT_THAT(topology.findPaths(node1, node2, NetworkTopology::Upstream), IsEmpty());
}

TEST_F(NetworkTopologyTest, FindPathSimpleDiamondShape)
{
    const auto src = WorkerId("src");
    const auto left = WorkerId("left");
    const auto right = WorkerId("right");
    const auto dest = WorkerId("dest");
    auto topology = NetworkTopology{};
    topology.addNode(src, {left, right});
    topology.addNode(left, {dest});
    topology.addNode(right, {dest});
    topology.addNode(dest, {});
    const auto paths = topology.findPaths(src, dest, NetworkTopology::Downstream);
    EXPECT_THAT(paths, SizeIs(2));
}

TEST_F(NetworkTopologyTest, FindPathInComplexTopology)
{
    /// Create a complex graph structure:
    ///           src
    ///          /   \
    ///       left1 right1
    ///      /   |  /    |
    ///   left2 mid2 right2
    ///      \    |    /
    ///         dest
    const auto src = WorkerId("src");
    const auto left1 = WorkerId("left1");
    const auto right1 = WorkerId("right1");
    const auto left2 = WorkerId("left2");
    const auto mid2 = WorkerId("mid2");
    const auto right2 = WorkerId("right2");
    const auto dest = WorkerId("dest");

    auto topology = NetworkTopology{};
    topology.addNode(src, {left1, right1});
    topology.addNode(left1, {left2, mid2});
    topology.addNode(right1, {mid2, right2});
    topology.addNode(left2, {dest});
    topology.addNode(mid2, {dest});
    topology.addNode(right2, {dest});
    topology.addNode(dest, {});

    EXPECT_THAT(
        topology.findPaths(src, dest, NetworkTopology::Downstream),
        UnorderedElementsAre(
            NetworkTopology::Path{{src, left1, left2, dest}},
            NetworkTopology::Path{{src, left1, mid2, dest}},
            NetworkTopology::Path{{src, right1, mid2, dest}},
            NetworkTopology::Path{{src, right1, right2, dest}}));

    EXPECT_THAT(
        topology.findPaths(left1, dest, NetworkTopology::Downstream),
        UnorderedElementsAre(NetworkTopology::Path{{left1, left2, dest}}, NetworkTopology::Path{{left1, mid2, dest}}));
}

TEST_F(NetworkTopologyTest, CycleDetectionDirectCycle)
{
    auto topology = NetworkTopology{};
    const auto a = WorkerId("a");
    const auto b = WorkerId("b");
    /// Add a with downstream=[b] before b exists (forward reference)
    topology.addNode(a, {b});
    /// Adding b with downstream=[a] would create: b → a → b (cycle)
    EXPECT_THROW(topology.addNode(b, {a}), Exception);
    EXPECT_THAT(topology, SizeIs(1));
}

TEST_F(NetworkTopologyTest, CycleDetectionIndirectCycle)
{
    auto topology = NetworkTopology{};
    const auto a = WorkerId("a");
    const auto b = WorkerId("b");
    const auto c = WorkerId("c");
    /// Build: a → c (forward ref), b → a
    topology.addNode(a, {c});
    topology.addNode(b, {a});
    /// Adding c with downstream=[b] would create: c → b → a → c (cycle)
    EXPECT_THROW(topology.addNode(c, {b}), Exception);
    EXPECT_THAT(topology, SizeIs(2));
}

TEST_F(NetworkTopologyTest, NoCycleDag)
{
    auto topology = NetworkTopology{};
    const auto a = WorkerId("a");
    const auto b = WorkerId("b");
    const auto c = WorkerId("c");
    const auto d = WorkerId("d");
    /// Diamond: a → b, a → c, b → d, c → d (no cycle)
    topology.addNode(d, {});
    topology.addNode(b, {d});
    topology.addNode(c, {d});
    EXPECT_NO_THROW(topology.addNode(a, {b, c}));
    EXPECT_THAT(topology, SizeIs(4));
}
}
