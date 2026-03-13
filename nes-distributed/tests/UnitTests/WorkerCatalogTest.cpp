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

#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

/// NOLINTBEGIN(google-build-using-namespace, readability-identifier-length)
namespace NES
{

using namespace ::testing;

class WorkerCatalogTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("WorkerCatalogTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup WorkerCatalogTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down WorkerCatalogTest class."); }
};

/// AddWorker: adding a worker returns true, size() increases by 1, getAllWorkers() contains it
TEST_F(WorkerCatalogTest, AddWorker)
{
    WorkerCatalog catalog;
    const auto host = Host("worker1:9090");
    const bool added = catalog.addWorker(host, "data1:4321", CapacityKind::Unlimited{}, {});
    EXPECT_TRUE(added);
    EXPECT_THAT(catalog.size(), Eq(1u));
    const auto workers = catalog.getAllWorkers();
    EXPECT_THAT(workers, SizeIs(1));
    EXPECT_THAT(workers[0].host, Eq(host));
}

/// AddWorkerDuplicate: adding same host twice returns false, size() stays at 1
TEST_F(WorkerCatalogTest, AddWorkerDuplicate)
{
    WorkerCatalog catalog;
    const auto host = Host("worker1:9090");
    const bool first = catalog.addWorker(host, "data1:4321", CapacityKind::Unlimited{}, {});
    const bool second = catalog.addWorker(host, "data1:4321", CapacityKind::Unlimited{}, {});
    EXPECT_TRUE(first);
    EXPECT_FALSE(second);
    EXPECT_THAT(catalog.size(), Eq(1u));
}

/// AddWorkerWithUnlimitedCapacity: worker with CapacityKind::Unlimited{} can be added and retrieved
TEST_F(WorkerCatalogTest, AddWorkerWithUnlimitedCapacity)
{
    WorkerCatalog catalog;
    const auto host = Host("worker1:9090");
    catalog.addWorker(host, "data1:4321", CapacityKind::Unlimited{}, {});
    const auto retrieved = catalog.getWorker(host);
    ASSERT_TRUE(retrieved.has_value());
    EXPECT_TRUE(std::holds_alternative<CapacityKind::Unlimited>(retrieved->maxOperators));
}

/// AddWorkerWithLimitedCapacity: worker with CapacityKind::Limited{100} can be added and retrieved
TEST_F(WorkerCatalogTest, AddWorkerWithLimitedCapacity)
{
    WorkerCatalog catalog;
    const auto host = Host("worker2:9090");
    catalog.addWorker(host, "data2:4321", CapacityKind::Limited{100}, {});
    const auto retrieved = catalog.getWorker(host);
    ASSERT_TRUE(retrieved.has_value());
    ASSERT_TRUE(std::holds_alternative<CapacityKind::Limited>(retrieved->maxOperators));
    EXPECT_THAT(std::get<CapacityKind::Limited>(retrieved->maxOperators).value, Eq(100u));
}

/// RemoveWorker: removing existing worker returns the WorkerConfig, size() decreases
TEST_F(WorkerCatalogTest, RemoveWorker)
{
    WorkerCatalog catalog;
    const auto host = Host("worker1:9090");
    catalog.addWorker(host, "data1:4321", CapacityKind::Unlimited{}, {});
    EXPECT_THAT(catalog.size(), Eq(1u));
    const auto removed = catalog.removeWorker(host);
    ASSERT_TRUE(removed.has_value());
    EXPECT_THAT(removed->host, Eq(host));
    EXPECT_THAT(catalog.size(), Eq(0u));
}

/// RemoveNonExistent: removing non-existent host returns nullopt, size unchanged
TEST_F(WorkerCatalogTest, RemoveNonExistent)
{
    WorkerCatalog catalog;
    const auto host = Host("worker1:9090");
    catalog.addWorker(host, "data1:4321", CapacityKind::Unlimited{}, {});
    const auto removed = catalog.removeWorker(Host("nonexistent:9090"));
    EXPECT_FALSE(removed.has_value());
    EXPECT_THAT(catalog.size(), Eq(1u));
}

/// GetAllWorkersEmpty: empty catalog returns empty vector
TEST_F(WorkerCatalogTest, GetAllWorkersEmpty)
{
    WorkerCatalog catalog;
    EXPECT_THAT(catalog.getAllWorkers(), IsEmpty());
}

/// GetAllWorkersMultiple: after adding N workers, getAllWorkers() returns N configs
TEST_F(WorkerCatalogTest, GetAllWorkersMultiple)
{
    WorkerCatalog catalog;
    catalog.addWorker(Host("worker1:9090"), "data1:4321", CapacityKind::Unlimited{}, {});
    catalog.addWorker(Host("worker2:9090"), "data2:4321", CapacityKind::Limited{50}, {});
    catalog.addWorker(Host("worker3:9090"), "data3:4321", CapacityKind::Limited{200}, {});
    EXPECT_THAT(catalog.getAllWorkers(), SizeIs(3));
    EXPECT_THAT(catalog.size(), Eq(3u));
}

/// TopologyConsistency: after addWorker with downstream, getTopology() reflects the edge
TEST_F(WorkerCatalogTest, TopologyConsistency)
{
    WorkerCatalog catalog;
    const auto upstream = Host("upstream:9090");
    const auto downstream = Host("downstream:9090");
    catalog.addWorker(downstream, "data-ds:4321", CapacityKind::Unlimited{}, {});
    catalog.addWorker(upstream, "data-us:4321", CapacityKind::Unlimited{}, {downstream});
    const auto topology = catalog.getTopology();
    EXPECT_THAT(topology.size(), Eq(2u));
    EXPECT_THAT(topology.getDownstreamNodesOf(upstream), Contains(downstream));
    EXPECT_THAT(topology.getUpstreamNodesOf(downstream), Contains(upstream));
}

/// TopologyAfterRemove: after removeWorker, getTopology() no longer contains that node
TEST_F(WorkerCatalogTest, TopologyAfterRemove)
{
    WorkerCatalog catalog;
    const auto host = Host("worker1:9090");
    catalog.addWorker(host, "data1:4321", CapacityKind::Unlimited{}, {});
    const auto topologyBefore = catalog.getTopology();
    EXPECT_TRUE(topologyBefore.contains(host));
    catalog.removeWorker(host);
    const auto topologyAfter = catalog.getTopology();
    EXPECT_FALSE(topologyAfter.contains(host));
    EXPECT_THAT(topologyAfter.size(), Eq(0u));
}

/// TopologyDiamond: source->left->sink and source->right->sink; topology has 4 nodes,
/// source has 2 downstream neighbors, sink has 2 upstream neighbors, and there are 2 paths
/// from source to sink.
TEST_F(WorkerCatalogTest, TopologyDiamond)
{
    WorkerCatalog catalog;
    const auto sourceNode = Host("source-node:8080");
    const auto leftNode = Host("left-node:8080");
    const auto rightNode = Host("right-node:8080");
    const auto sinkNode = Host("sink-node:8080");

    catalog.addWorker(sinkNode, "data-sink-node:4321", CapacityKind::Unlimited{}, {});
    catalog.addWorker(leftNode, "data-left-node:4321", CapacityKind::Unlimited{}, {sinkNode});
    catalog.addWorker(rightNode, "data-right-node:4321", CapacityKind::Unlimited{}, {sinkNode});
    catalog.addWorker(sourceNode, "data-source-node:4321", CapacityKind::Unlimited{}, {leftNode, rightNode});

    EXPECT_THAT(catalog.size(), Eq(4u));
    const auto topology = catalog.getTopology();
    EXPECT_THAT(topology.size(), Eq(4u));

    EXPECT_THAT(topology.getDownstreamNodesOf(sourceNode), UnorderedElementsAre(leftNode, rightNode));
    EXPECT_THAT(topology.getUpstreamNodesOf(sinkNode), UnorderedElementsAre(leftNode, rightNode));

    const auto paths = topology.findPaths(sourceNode, sinkNode, NetworkTopology::Downstream);
    EXPECT_THAT(paths, SizeIs(2));
}

/// TopologyShortcut: source->mid->sink and source->sink directly; topology has 3 nodes,
/// source downstream contains both mid and sink, sink upstream contains both mid and source,
/// and there are 2 paths from source to sink.
TEST_F(WorkerCatalogTest, TopologyShortcut)
{
    WorkerCatalog catalog;
    const auto sourceNode = Host("source-node:8080");
    const auto midNode = Host("mid-node:8080");
    const auto sinkNode = Host("sink-node:8080");

    catalog.addWorker(sinkNode, "data-sink-node:4321", CapacityKind::Unlimited{}, {});
    catalog.addWorker(midNode, "data-mid-node:4321", CapacityKind::Unlimited{}, {sinkNode});
    catalog.addWorker(sourceNode, "data-source-node:4321", CapacityKind::Unlimited{}, {midNode, sinkNode});

    EXPECT_THAT(catalog.size(), Eq(3u));
    const auto topology = catalog.getTopology();
    EXPECT_THAT(topology.size(), Eq(3u));

    EXPECT_THAT(topology.getDownstreamNodesOf(sourceNode), UnorderedElementsAre(midNode, sinkNode));
    EXPECT_THAT(topology.getUpstreamNodesOf(sinkNode), UnorderedElementsAre(midNode, sourceNode));

    const auto paths = topology.findPaths(sourceNode, sinkNode, NetworkTopology::Downstream);
    EXPECT_THAT(paths, SizeIs(2));
}

/// TopologyCycle: NetworkTopology enforces DAG invariants. Attempting to add a cycle-completing
/// edge (node-c->node-a when node-a->node-b->node-c already exists) throws an exception.
/// Two valid edges are stored before the cycle is detected; after the exception the partial
/// topology contains 2 nodes.
TEST_F(WorkerCatalogTest, TopologyCycle)
{
    WorkerCatalog catalog;
    const auto nodeA = Host("node-a:8080");
    const auto nodeB = Host("node-b:8080");
    const auto nodeC = Host("node-c:8080");

    /// Build: node-a -> node-b -> node-c (valid DAG edges)
    catalog.addWorker(nodeA, "data-node-a:4321", CapacityKind::Unlimited{}, {nodeB});
    catalog.addWorker(nodeB, "data-node-b:4321", CapacityKind::Unlimited{}, {nodeC});

    /// Adding node-c with downstream=[node-a] would close a cycle: node-c -> node-a -> node-b -> node-c
    EXPECT_THROW(catalog.addWorker(nodeC, "data-node-c:4321", CapacityKind::Unlimited{}, {nodeA}), Exception);

    /// The topology rejects the cycle-closing node; only 2 nodes appear in the topology.
    const auto topology = catalog.getTopology();
    EXPECT_THAT(topology.size(), Eq(2u));
    EXPECT_FALSE(topology.contains(nodeC));

    EXPECT_THAT(topology.getDownstreamNodesOf(nodeA), Contains(nodeB));
    EXPECT_THAT(topology.getDownstreamNodesOf(nodeB), SizeIs(1));
}

/// TopologyDiamondRemoveIntermediateNode: after removing left-node from the diamond, only
/// 1 path remains from source to sink (via right-node), and left-node is no longer in the topology.
TEST_F(WorkerCatalogTest, TopologyDiamondRemoveIntermediateNode)
{
    WorkerCatalog catalog;
    const auto sourceNode = Host("source-node:8080");
    const auto leftNode = Host("left-node:8080");
    const auto rightNode = Host("right-node:8080");
    const auto sinkNode = Host("sink-node:8080");

    catalog.addWorker(sinkNode, "data-sink-node:4321", CapacityKind::Unlimited{}, {});
    catalog.addWorker(leftNode, "data-left-node:4321", CapacityKind::Unlimited{}, {sinkNode});
    catalog.addWorker(rightNode, "data-right-node:4321", CapacityKind::Unlimited{}, {sinkNode});
    catalog.addWorker(sourceNode, "data-source-node:4321", CapacityKind::Unlimited{}, {leftNode, rightNode});

    catalog.removeWorker(leftNode);

    EXPECT_THAT(catalog.size(), Eq(3u));
    const auto topology = catalog.getTopology();
    EXPECT_THAT(topology.size(), Eq(3u));
    EXPECT_FALSE(topology.contains(leftNode));

    EXPECT_THAT(topology.getDownstreamNodesOf(sourceNode), Not(Contains(leftNode)));

    const auto paths = topology.findPaths(sourceNode, sinkNode, NetworkTopology::Downstream);
    EXPECT_THAT(paths, SizeIs(1));
}

}

/// NOLINTEND(google-build-using-namespace, readability-identifier-length)
