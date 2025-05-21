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

#include <Distributed/Topology.hpp>

#include <tuple>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock.h>
#include <BaseUnitTest.hpp>

namespace NES::Distributed
{
class TopologyTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TopologyTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyTest class.");
    }


    static void TearDownTestSuite() { NES_INFO("Tear down TopologyTest class."); }
};

TEST_F(TopologyTest, AddNodeToToplogy)
{
    Topology topology;
    auto node = topology.addNode();
    EXPECT_THAT(topology, ::testing::SizeIs(1));
}
TEST_F(TopologyTest, AddAndRemoveNode)
{
    Topology topology;
    auto node = topology.addNode();
    topology.removeNode(node);
    EXPECT_THAT(topology, ::testing::IsEmpty());
}

TEST_F(TopologyTest, SingleNodeWithoutUpOrDownstream)
{
    Topology topology;
    auto node = topology.addNode();
    EXPECT_THAT(topology.getDownstreams(node), ::testing::IsEmpty());
    EXPECT_THAT(topology.getUpstreams(node), ::testing::IsEmpty());
}


TEST_F(TopologyTest, TwoNodesWithoutUpOrDownstream)
{
    Topology topology;
    auto node1 = topology.addNode();
    auto node2 = topology.addNode();
    EXPECT_THAT(topology.getDownstreams(node1), ::testing::IsEmpty());
    EXPECT_THAT(topology.getUpstreams(node1), ::testing::IsEmpty());
    EXPECT_THAT(topology.getDownstreams(node2), ::testing::IsEmpty());
    EXPECT_THAT(topology.getUpstreams(node2), ::testing::IsEmpty());
}

TEST_F(TopologyTest, TwoNodesConnection)
{
    Topology topology;
    auto node1 = topology.addNode();
    auto node2 = topology.addNode();
    topology.addUpstreams(node1, node2);

    EXPECT_THAT(topology.getDownstreams(node1), ::testing::IsEmpty());
    EXPECT_THAT(topology.getUpstreams(node1), ::testing::Contains(node2));
    EXPECT_THAT(topology.getDownstreams(node2), ::testing::Contains(node1));
    EXPECT_THAT(topology.getUpstreams(node2), ::testing::IsEmpty());
}

TEST_F(TopologyTest, TwoNodesConnectionRemoveAgain)
{
    Topology topology;
    auto node1 = topology.addNode();
    auto node2 = topology.addNode();
    topology.addUpstreams(node1, node2);
    topology.removeNode(node2);

    EXPECT_THAT(topology.getDownstreams(node1), ::testing::IsEmpty());
    EXPECT_THAT(topology.getUpstreams(node1), ::testing::IsEmpty());
    EXPECT_ANY_THROW(topology.getDownstreams(node2));
    EXPECT_ANY_THROW(topology.getUpstreams(node2));
}


/// Paths
TEST_F(TopologyTest, FindPathSimpleTwoNodes)
{
    Topology topology;
    auto node1 = topology.addNode();
    auto node2 = topology.addNode();
    topology.addUpstreams(node1, node2);

    EXPECT_THAT(topology.findPaths(node1, node2, Topology::Upstream), ::testing::Contains(Topology::Path{{node1, node2}}));
    EXPECT_THAT(topology.findPaths(node2, node1, Topology::Upstream), ::testing::IsEmpty());

    EXPECT_THAT(topology.findPaths(node2, node1, Topology::Downstream), ::testing::Contains(Topology::Path{{node2, node1}}));
    EXPECT_THAT(topology.findPaths(node1, node2, Topology::Downstream), ::testing::IsEmpty());
}

TEST_F(TopologyTest, FindPathSimpleDiamondShape)
{
    Topology topology;
    auto node1 = topology.addNode();
    auto node21 = topology.addNode();
    auto node22 = topology.addNode();
    auto node3 = topology.addNode();

    topology.addUpstreams(node1, node21, node22);
    topology.addDownstreams(node3, node21, node22);

    EXPECT_THAT(
        topology.findPaths(node1, node3, Topology::Upstream),
        ::testing::UnorderedElementsAre(Topology::Path{{node1, node21, node3}}, Topology::Path{{node1, node22, node3}}));
    EXPECT_THAT(topology.findPaths(node21, node3, Topology::Upstream), ::testing::UnorderedElementsAre(Topology::Path{{node21, node3}}));
}

TEST_F(TopologyTest, FindPathSimpleDiamondShapeRemoveNode)
{
    Topology topology;
    auto node1 = topology.addNode();
    auto node21 = topology.addNode();
    auto node22 = topology.addNode();
    auto node3 = topology.addNode();

    topology.addUpstreams(node1, node21, node22);
    topology.addDownstreams(node3, node21, node22);
    topology.removeNode(node22);


    EXPECT_THAT(
        topology.findPaths(node1, node3, Topology::Upstream), ::testing::UnorderedElementsAre(Topology::Path{{node1, node21, node3}}));
    EXPECT_THAT(topology.findPaths(node21, node3, Topology::Upstream), ::testing::UnorderedElementsAre(Topology::Path{{node21, node3}}));
}

TEST_F(TopologyTest, FindPathInAComplexTopology)
{
    Topology topology;

    auto node1 = topology.addNode();
    auto node2 = topology.addNode();
    auto node3 = topology.addNode();
    auto node4 = topology.addNode();
    auto node5 = topology.addNode();
    auto node6 = topology.addNode();
    auto node7 = topology.addNode();

    /// Create a complex graph structure:
    ///           node1
    ///          /     \
    ///       node2   node3
    ///      /  |    /  |
    ///   node4 node5 node6
    ///      \   |   /
    ///         node7

    topology.addUpstreams(node1, node2, node3);
    topology.addUpstreams(node2, node4, node5);
    topology.addUpstreams(node3, node5, node6);
    topology.addDownstreams(node7, node4, node5, node6);

    EXPECT_THAT(
        topology.findPaths(node1, node7, Topology::Upstream),
        ::testing::UnorderedElementsAre(
            Topology::Path{{node1, node2, node4, node7}},
            Topology::Path{{node1, node2, node5, node7}},
            Topology::Path{{node1, node3, node5, node7}},
            Topology::Path{{node1, node3, node6, node7}}));

    EXPECT_THAT(
        topology.findPaths(node2, node7, Topology::Upstream),
        ::testing::UnorderedElementsAre(Topology::Path{{node2, node4, node7}}, Topology::Path{{node2, node5, node7}}));

    /// Remove a middle node and verify paths are updated
    topology.removeNode(node5);
    EXPECT_THAT(
        topology.findPaths(node1, node7, Topology::Upstream),
        ::testing::UnorderedElementsAre(Topology::Path{{node1, node2, node4, node7}}, Topology::Path{{node1, node3, node6, node7}}));

    EXPECT_THAT(topology.findPaths(node4, node7, Topology::Upstream), ::testing::UnorderedElementsAre(Topology::Path{{node4, node7}}));
}

/// Common Node
TEST_F(TopologyTest, FindCommonNodeInAComplexTopology)
{
    Topology topology;

    auto node1 = topology.addNode();
    auto node2 = topology.addNode();
    auto node3 = topology.addNode();
    auto node4 = topology.addNode();
    auto node5 = topology.addNode();
    auto node6 = topology.addNode();
    auto node7 = topology.addNode();

    /// Create a complex graph structure:
    ///           node1
    ///          /     \
    ///       node2   node3
    ///      /  |    /  |
    ///   node4 node5 node6
    ///      \   |   /
    ///         node7

    topology.addUpstreams(node1, node2, node3);
    topology.addUpstreams(node2, node4, node5);
    topology.addUpstreams(node3, node5, node6);
    topology.addDownstreams(node7, node4, node5, node6);

    EXPECT_THAT(
        topology.findCommonNode(node2, node3, node7, Topology::Upstream),
        ::testing::UnorderedElementsAre(std::tuple{node5, Topology::Path{{node2, node5, node7}}, Topology::Path{{node3, node5, node7}}}));

    EXPECT_THAT(
        topology.findCommonNode(node1, node1, node1, Topology::Upstream),
        ::testing::UnorderedElementsAre(std::make_tuple(node1, Topology::Path{{node1}}, Topology::Path{{node1}})));

    EXPECT_THAT(
        topology.findCommonNode(node1, node2, node2, Topology::Upstream),
        ::testing::UnorderedElementsAre(std::make_tuple(node2, Topology::Path{{node1, node2}}, Topology::Path{{node2}})));

    EXPECT_THAT(topology.findCommonNode(node2, node7, node4, Topology::Upstream), ::testing::IsEmpty());

    EXPECT_THAT(
        topology.findCommonNode(node4, node6, node1, Topology::Downstream),
        ::testing::UnorderedElementsAre(std::tuple{node1, Topology::Path{{node4, node2, node1}}, Topology::Path{{node6, node3, node1}}}));

    /// Add additional option:
    ///           node1
    ///          /     \
    ///       node2   node3
    ///      /       /  |
    ///   node4 node5->node6
    ///          |   /
    ///         node7

    topology.removeNode(node4);
    node4 = topology.addNode();
    topology.addDownstreams(node4, node2);

    topology.removeNode(node5);
    node5 = topology.addNode();
    topology.addUpstreams(node5, node7, node6);
    topology.addDownstreams(node5, node3);


    EXPECT_THAT(
        topology.findPaths(node7, node1, Topology::Downstream),
        ::testing::UnorderedElementsAre(
            Topology::Path{{node7, node5, node3, node1}},
            Topology::Path{{node7, node6, node3, node1}},
            Topology::Path{{node7, node6, node5, node3, node1}}));

    EXPECT_THAT(
        topology.findCommonNode(node4, node7, node1, Topology::Downstream),
        ::testing::UnorderedElementsAre(
            std::tuple{node1, Topology::Path{{node4, node2, node1}}, Topology::Path{{node7, node6, node3, node1}}},
            std::tuple{node1, Topology::Path{{node4, node2, node1}}, Topology::Path{{node7, node5, node3, node1}}},
            std::tuple{node1, Topology::Path{{node4, node2, node1}}, Topology::Path{{node7, node6, node5, node3, node1}}}));
}
}
