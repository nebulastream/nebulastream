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
    const auto node = HostAddr("host");
    auto topology = Topology{};
    topology.addNode(node, {});
    EXPECT_THAT(topology, SizeIs(1));
    EXPECT_THAT(topology.getDownstreamNodesOf(node), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node), IsEmpty());
}

TEST_F(NetworkTopologyTest, TwoNodesWithoutUpOrDownstream)
{
    const auto node1 = HostAddr("node1");
    const auto node2 = HostAddr("node2");
    auto topology = Topology{};
    topology.addNode(node1, {});
    topology.addNode(node2, {});
    EXPECT_THAT(topology.getDownstreamNodesOf(node1), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node1), IsEmpty());
    EXPECT_THAT(topology.getDownstreamNodesOf(node2), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node2), IsEmpty());
}

TEST_F(NetworkTopologyTest, TwoNodesConnection)
{
    const auto node1 = HostAddr("node1");
    const auto node2 = HostAddr("node2");
    auto topology = Topology{};
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
    const auto node1 = HostAddr("node1");
    const auto node2 = HostAddr("node2");
    auto topology = Topology{};
    topology.addNode(node1, {node2});
    topology.addNode(node2, {});
    EXPECT_THAT(topology.findPaths(node1, node2, Topology::Downstream), Contains(Topology::Path{{node1, node2}}));
    EXPECT_THAT(topology.findPaths(node2, node1, Topology::Downstream), IsEmpty());

    EXPECT_THAT(topology.findPaths(node2, node1, Topology::Upstream), Contains(Topology::Path{{node2, node1}}));
    EXPECT_THAT(topology.findPaths(node1, node2, Topology::Upstream), IsEmpty());
}

TEST_F(NetworkTopologyTest, FindPathSimpleDiamondShape)
{
    const auto src = HostAddr("src");
    const auto left = HostAddr("left");
    const auto right = HostAddr("right");
    const auto dest = HostAddr("dest");
    auto topology = Topology{};
    topology.addNode(src, {left, right});
    topology.addNode(left, {dest});
    topology.addNode(right, {dest});
    topology.addNode(dest, {});
    const auto paths = topology.findPaths(src, dest, Topology::Downstream);
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
    const auto src = HostAddr("src");
    const auto left1 = HostAddr("left1");
    const auto right1 = HostAddr("right1");
    const auto left2 = HostAddr("left2");
    const auto mid2 = HostAddr("mid2");
    const auto right2 = HostAddr("right2");
    const auto dest = HostAddr("dest");

    auto topology = Topology{};
    topology.addNode(src, {left1, right1});
    topology.addNode(left1, {left2, mid2});
    topology.addNode(right1, {mid2, right2});
    topology.addNode(left2, {dest});
    topology.addNode(mid2, {dest});
    topology.addNode(right2, {dest});
    topology.addNode(dest, {});

    EXPECT_THAT(
        topology.findPaths(src, dest, Topology::Downstream),
        UnorderedElementsAre(
            Topology::Path{{src, left1, left2, dest}},
            Topology::Path{{src, left1, mid2, dest}},
            Topology::Path{{src, right1, mid2, dest}},
            Topology::Path{{src, right1, right2, dest}}));

    EXPECT_THAT(
        topology.findPaths(left1, dest, Topology::Downstream),
        UnorderedElementsAre(Topology::Path{{left1, left2, dest}}, Topology::Path{{left1, mid2, dest}}));
}
}
