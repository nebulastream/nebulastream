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

#include <Distributed/NetworkTopology.hpp>

#include <gmock/gmock.h>

#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{

using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;
using ::testing::Field;
using ::testing::Pair;
using ::testing::IsEmpty;
using ::testing::Contains;
using ::testing::SizeIs;

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
    const auto topology = TopologyGraph::from({{node, {}}});
    EXPECT_THAT(topology, SizeIs(1));
    EXPECT_THAT(topology.getDownstreamNodesOf(node), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node), IsEmpty());
}

TEST_F(NetworkTopologyTest, TwoNodesWithoutUpOrDownstream)
{
    const std::string node1 = "node1";
    const std::string node2 = "node2";
    const auto topology = TopologyGraph::from({{node1, {}}, {node2, {}}});
    EXPECT_THAT(topology.getDownstreamNodesOf(node1), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node1), IsEmpty());
    EXPECT_THAT(topology.getDownstreamNodesOf(node2), IsEmpty());
    EXPECT_THAT(topology.getUpstreamNodesOf(node2), IsEmpty());
}

TEST_F(NetworkTopologyTest, TwoNodesConnection)
{
    const std::string node1 = "node1";
    const std::string node2 = "node2";
    const auto topology = TopologyGraph::from({{node1, {node2}}, {node2, {}}});

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
    const auto topology = TopologyGraph::from({{node1, {node2}}, {node2, {}}});

    EXPECT_THAT(topology.findPaths(node1, node2, TopologyGraph::Downstream), Contains(TopologyGraph::Path{{node1, node2}}));
    EXPECT_THAT(topology.findPaths(node2, node1, TopologyGraph::Downstream), IsEmpty());

    EXPECT_THAT(topology.findPaths(node2, node1, TopologyGraph::Upstream), Contains(TopologyGraph::Path{{node2, node1}}));
    EXPECT_THAT(topology.findPaths(node1, node2, TopologyGraph::Upstream), IsEmpty());
}


TEST_F(NetworkTopologyTest, FindPathSimpleDiamondShape)
{
    const std::string src = "src";
    const std::string left = "left";
    const std::string right = "right";
    const std::string dest = "dest";
    const auto topology = TopologyGraph::from({{src, {left, right}}, {left, {dest}}, {right, {dest}}, {dest, {}}});

    const auto paths = topology.findPaths(src, dest, TopologyGraph::Downstream);
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

    const std::vector<std::pair<TopologyGraph::NodeId, std::vector<TopologyGraph::NodeId>>> dag
        = {{src, {left1, right1}},
           {left1, {left2, mid2}},
           {right1, {mid2, right2}},
           {left2, {dest}},
           {mid2, {dest}},
           {right2, {dest}},
           {dest, {}}};
    const auto topology = TopologyGraph::from(std::move(dag));

    EXPECT_THAT(
        topology.findPaths(src, dest, TopologyGraph::Downstream),
        UnorderedElementsAre(
            TopologyGraph::Path{{src, left1, left2, dest}},
            TopologyGraph::Path{{src, left1, mid2, dest}},
            TopologyGraph::Path{{src, right1, mid2, dest}},
            TopologyGraph::Path{{src, right1, right2, dest}}));

    EXPECT_THAT(
        topology.findPaths(left1, dest, TopologyGraph::Downstream),
        UnorderedElementsAre(TopologyGraph::Path{{left1, left2, dest}}, TopologyGraph::Path{{left1, mid2, dest}}));
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

    const std::vector<std::pair<TopologyGraph::NodeId, std::vector<TopologyGraph::NodeId>>> dag
        = {{src, {left1, right1}},
           {left1, {left2, mid2}},
           {right1, {mid2, right2}},
           {left2, {dest}},
           {mid2, {dest}},
           {right2, {dest}},
           {dest, {}}};
    const auto topology = TopologyGraph::from(std::move(dag));

    EXPECT_THAT(topology.findLCA(left1, right1, dest, TopologyGraph::Downstream),
        ElementsAre(
            AllOf(
                Field(&TopologyGraph::LowestCommonAncestor::lca, mid2),
                Field(&TopologyGraph::LowestCommonAncestor::paths,
                    Pair(
                        Field(&TopologyGraph::Path::path, ElementsAre(left1, mid2, dest)),
                        Field(&TopologyGraph::Path::path, ElementsAre(right1, mid2, dest))
                    )
                )
            )
        )
    );

    EXPECT_THAT(topology.findLCA(src, src, src, TopologyGraph::Downstream),
        ElementsAre(
            AllOf(
                Field(&TopologyGraph::LowestCommonAncestor::lca, src),
                Field(&TopologyGraph::LowestCommonAncestor::paths,
                    Pair(
                        Field(&TopologyGraph::Path::path, ElementsAre(src)),
                        Field(&TopologyGraph::Path::path, ElementsAre(src))
                    )
                )
            )
        )
    );

    EXPECT_THAT(topology.findLCA(src, left1, left1, TopologyGraph::Downstream),
        ElementsAre(
            AllOf(
                Field(&TopologyGraph::LowestCommonAncestor::lca, left1),
                Field(&TopologyGraph::LowestCommonAncestor::paths,
                    Pair(
                        Field(&TopologyGraph::Path::path, ElementsAre(src, left1)),
                        Field(&TopologyGraph::Path::path, ElementsAre(left1))
                    )
                )
            )
        )
    );

    EXPECT_THAT(topology.findLCA(left1, dest, left2, TopologyGraph::Upstream), IsEmpty());

    EXPECT_THAT(topology.findLCA(left2, right2, src, TopologyGraph::Upstream),
        ElementsAre(
            AllOf(
                Field(&TopologyGraph::LowestCommonAncestor::lca, src),
                Field(&TopologyGraph::LowestCommonAncestor::paths,
                    Pair(
                        Field(&TopologyGraph::Path::path, ElementsAre(left2, left1, src)),
                        Field(&TopologyGraph::Path::path, ElementsAre(right2, right1, src))
                    )
                )
            )
        )
    );
}
}
