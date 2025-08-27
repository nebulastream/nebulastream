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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{

using ::testing::Contains;
using ::testing::ElementsAre;
using ::testing::Field;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

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
    const std::string node = "host";
    auto topology = Topology{};
    topology.addNode(node, {});
    EXPECT_THAT(topology, SizeIs(1));
    EXPECT_THAT(topology.getDownstreamNodesOf(node), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node), IsEmpty());
}

TEST_F(NetworkTopologyTest, TwoNodesWithoutUpOrDownstream)
{
    const std::string node1 = "node1";
    const std::string node2 = "node2";
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
    const std::string node1 = "node1";
    const std::string node2 = "node2";
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
    const std::string node1 = "node1";
    const std::string node2 = "node2";
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
    const std::string src = "src";
    const std::string left = "left";
    const std::string right = "right";
    const std::string dest = "dest";
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
    const std::string src = "src";
    const std::string left1 = "left1";
    const std::string right1 = "right1";
    const std::string left2 = "left2";
    const std::string mid2 = "mid2";
    const std::string right2 = "right2";
    const std::string dest = "dest";

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

TEST_F(NetworkTopologyTest, FindLCAInComplexTopology)
{
    /// Create a complex graph structure:
    ///           src
    ///          /   \
    ///       left1 right1
    ///      /   |  /    |
    ///   left2 mid2 right2
    ///      \    |    /
    ///         dest
    const std::string src = "src";
    const std::string left1 = "left1";
    const std::string right1 = "right1";
    const std::string left2 = "left2";
    const std::string mid2 = "mid2";
    const std::string right2 = "right2";
    const std::string dest = "dest";

    auto topology = Topology{};
    topology.addNode(src, {left1, right1});
    topology.addNode(left1, {left2, mid2});
    topology.addNode(right1, {mid2, right2});
    topology.addNode(left2, {dest});
    topology.addNode(mid2, {dest});
    topology.addNode(right2, {dest});
    topology.addNode(dest, {});

    EXPECT_THAT(
        topology.findLCA(left1, right1, dest, Topology::Downstream),
        ElementsAre(AllOf(
            Field(&Topology::LowestCommonAncestor::lca, mid2),
            Field(
                &Topology::LowestCommonAncestor::paths,
                Pair(
                    Field(&Topology::Path::path, ElementsAre(left1, mid2, dest)),
                    Field(&Topology::Path::path, ElementsAre(right1, mid2, dest)))))));

    EXPECT_THAT(
        topology.findLCA(src, src, src, Topology::Downstream),
        ElementsAre(AllOf(
            Field(&Topology::LowestCommonAncestor::lca, src),
            Field(
                &Topology::LowestCommonAncestor::paths,
                Pair(Field(&Topology::Path::path, ElementsAre(src)), Field(&Topology::Path::path, ElementsAre(src)))))));

    EXPECT_THAT(
        topology.findLCA(src, left1, left1, Topology::Downstream),
        ElementsAre(AllOf(
            Field(&Topology::LowestCommonAncestor::lca, left1),
            Field(
                &Topology::LowestCommonAncestor::paths,
                Pair(Field(&Topology::Path::path, ElementsAre(src, left1)), Field(&Topology::Path::path, ElementsAre(left1)))))));

    EXPECT_THAT(topology.findLCA(left1, dest, left2, Topology::Upstream), IsEmpty());

    EXPECT_THAT(
        topology.findLCA(left2, right2, src, Topology::Upstream),
        ElementsAre(AllOf(
            Field(&Topology::LowestCommonAncestor::lca, src),
            Field(
                &Topology::LowestCommonAncestor::paths,
                Pair(
                    Field(&Topology::Path::path, ElementsAre(left2, left1, src)),
                    Field(&Topology::Path::path, ElementsAre(right2, right1, src)))))));
}

/// DAG Validation Tests
TEST_F(NetworkTopologyTest, ValidDAGSimple)
{
    const std::string node1 = "node1";
    const std::string node2 = "node2";
    auto topology = Topology{};
    topology.addNode(node1, {node2});
    topology.addNode(node2, {});
    
    EXPECT_TRUE(topology.isValidDAG());
}

TEST_F(NetworkTopologyTest, ValidDAGDiamond)
{
    const std::string src = "src";
    const std::string left = "left";
    const std::string right = "right";
    const std::string sink = "sink";
    auto topology = Topology{};
    topology.addNode(src, {left, right});
    topology.addNode(left, {sink});
    topology.addNode(right, {sink});
    topology.addNode(sink, {});
    
    EXPECT_TRUE(topology.isValidDAG());
}

TEST_F(NetworkTopologyTest, InvalidDAGWithCycle)
{
    const std::string node1 = "node1";
    const std::string node2 = "node2";
    const std::string node3 = "node3";
    auto topology = Topology{};
    topology.addNode(node1, {node2});
    topology.addNode(node2, {node3});
    topology.addNode(node3, {node1}); /// Creates cycle: node1 -> node2 -> node3 -> node1
    
    EXPECT_FALSE(topology.isValidDAG());
}

TEST_F(NetworkTopologyTest, InvalidDAGWithSubtleCycle)
{
    /// Complex topology with a subtle cycle embedded within legitimate paths
    /// src -> left -> middle1 -> middle2 -> sink
    ///     -> right -> middle2 -> middle1 (creates cycle between middle1 and middle2)
    const std::string src = "src";
    const std::string left = "left";
    const std::string right = "right";
    const std::string middle1 = "middle1";
    const std::string middle2 = "middle2";
    const std::string sink = "sink";
    
    auto topology = Topology{};
    topology.addNode(src, {left, right});
    topology.addNode(left, {middle1});
    topology.addNode(right, {middle2});
    topology.addNode(middle1, {middle2}); /// Forward edge
    topology.addNode(middle2, {middle1, sink}); /// Back edge creates cycle: middle1 -> middle2 -> middle1
    topology.addNode(sink, {});
    
    EXPECT_FALSE(topology.isValidDAG());
}
}
