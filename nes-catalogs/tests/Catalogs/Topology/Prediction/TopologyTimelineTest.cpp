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
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Topology/Prediction/Edge.hpp>
#include <Catalogs/Topology/Prediction/TopologyDelta.hpp>
#include <Catalogs/Topology/Prediction/TopologyTimeline.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {
using Experimental::TopologyPrediction::Edge;
using Experimental::TopologyPrediction::TopologyTimeline;
class TopologyTimelineTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TopologyTimelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TopologyTimeline test class.");
    }

    static std::vector<WorkerId> getIdVector(const std::vector<NodePtr>& nodes) {
        std::vector<WorkerId> ids;
        ids.reserve(nodes.size());
        for (auto& node : nodes) {
            ids.push_back(node->as<TopologyNode>()->getId());
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

    static void compareIdVectors(const std::vector<NodePtr>& original, const std::vector<NodePtr>& copy) {
        EXPECT_EQ(original.size(), copy.size());
        std::vector<WorkerId> originalIds = getIdVector(original);
        std::vector<WorkerId> copiedIds = getIdVector(copy);

        std::sort(originalIds.begin(), originalIds.end());
        std::sort(copiedIds.begin(), copiedIds.end());

        ASSERT_EQ(originalIds, copiedIds);
    }

    static void compareNodeAt(Experimental::TopologyPrediction::TopologyTimeline versions,
                              Timestamp time,
                              uint64_t id,
                              std::vector<uint64_t> parents,
                              std::vector<uint64_t> children) {
        NES_DEBUG("compare node {}", id);
        auto predictedNode = versions.getTopologyVersion(time)->getCopyOfTopologyNodeWithId(id);

        //check if the node is predicted to be removed
        if (children.empty() && parents.empty()) {
            EXPECT_EQ(predictedNode, nullptr);
            return;
        }

        //compare links
        std::sort(children.begin(), children.end());
        std::sort(parents.begin(), parents.end());

        EXPECT_EQ(getIdVector(predictedNode->getChildren()), children);
        EXPECT_EQ(getIdVector(predictedNode->getParents()), parents);
    }

    static void testTopologyEquality(const TopologyPtr& original, const TopologyPtr& copy) {
        NES_DEBUG("comparing topologies");
        (void) copy;

        std::queue<TopologyNodePtr> originalQueue;
        originalQueue.push(original->getRoot());
        std::set<uint64_t> visited;

        while (!originalQueue.empty()) {
            auto originalNode = originalQueue.front();
            originalQueue.pop();
            ASSERT_TRUE(originalNode);
            NES_DEBUG("checking node {}", originalNode->getId());
            auto copiedNode = copy->getCopyOfTopologyNodeWithId(originalNode->getId());
            NES_DEBUG("copied node id {}", copiedNode->getId());
            ASSERT_NE(copiedNode, nullptr);

            ASSERT_NE(originalNode, copiedNode);

            compareIdVectors(originalNode->getChildren(), copiedNode->getChildren());
            compareIdVectors(originalNode->getParents(), copiedNode->getParents());

            for (const auto& child : originalNode->getChildren()) {
                auto id = child->as<TopologyNode>()->getId();
                if (!visited.contains(id)) {
                    visited.insert(id);
                    originalQueue.push(child->as<TopologyNode>());
                }
            }
        }
    }
};

TEST_F(TopologyTimelineTest, DISABLED_testNoChangesPresent) {
    auto topology = Topology::create();
    std::map<std::string, std::any> properties;
    topology->setRootTopologyNodeId(TopologyNode::create(1, "localhost", 4001, 5001, 4, properties));
    topology->addNewTopologyNodeAsChild(topology->getCopyOfTopologyNodeWithId(1),
                                        TopologyNode::create(2, "localhost", 4001, 5001, 4, properties));
    topology->addNewTopologyNodeAsChild(topology->getCopyOfTopologyNodeWithId(2),
                                        TopologyNode::create(3, "localhost", 4001, 5001, 4, properties));
    topology->addNewTopologyNodeAsChild(topology->getCopyOfTopologyNodeWithId(2),
                                        TopologyNode::create(4, "localhost", 4001, 5001, 4, properties));

    auto timeline = TopologyTimeline::create(topology);
    auto changedTopology = timeline->getTopologyVersion(23);
    ASSERT_NE(changedTopology, topology);
    testTopologyEquality(changedTopology, topology);
}

TEST_F(TopologyTimelineTest, DISABLED_testUpdatingMultiplePredictions) {
    auto topology = Topology::create();
    std::map<std::string, std::any> properties;
    topology->setRootTopologyNodeId(TopologyNode::create(1, "localhost", 4001, 5001, 4, properties));
    topology->addNewTopologyNodeAsChild(topology->getCopyOfTopologyNodeWithId(1),
                                        TopologyNode::create(2, "localhost", 4001, 5001, 4, properties));
    topology->addNewTopologyNodeAsChild(topology->getCopyOfTopologyNodeWithId(2),
                                        TopologyNode::create(3, "localhost", 4001, 5001, 4, properties));
    topology->addNewTopologyNodeAsChild(topology->getCopyOfTopologyNodeWithId(2),
                                        TopologyNode::create(4, "localhost", 4001, 5001, 4, properties));
    NES_DEBUG("Original Topology");
    NES_DEBUG("{}", topology->toString());
    Experimental::TopologyPrediction::TopologyTimeline versionTimeline(topology);

    //create new prediction for node 3 to reconnect to node 4
    NES_DEBUG("adding new prediction that node 3 will reconnect to node 4 at time 7");
    Experimental::TopologyPrediction::TopologyDelta delta3({{3, 4}}, {{3, 2}});
    versionTimeline.addTopologyDelta(7, delta3);

    Timestamp viewTime;

    //check values
    //view at time 6
    viewTime = 6;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {3, 4});
    compareNodeAt(versionTimeline, viewTime, 3, {2}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {});

    //view at time 7
    viewTime = 7;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {4});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {3});

    //changing time of prediction of node 3 to happen at 4 instead of 7
    NES_DEBUG("updating prediction that node 3 will reconnect to node 4 at time 4 instead of time 7");
    versionTimeline.removeTopologyDelta(7, delta3);
    versionTimeline.addTopologyDelta(4, delta3);

    //check values
    //view at time 3
    viewTime = 3;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {3, 4});
    compareNodeAt(versionTimeline, viewTime, 3, {2}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {});

    //view at time 4
    viewTime = 4;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {4});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {3});

    //adding new prediction for node 4 to reconnect to node 1 at time 6
    NES_DEBUG("adding new prediction that node 4 will reconnect to node 1 at time 6");
    Experimental::TopologyPrediction::TopologyDelta delta4({{4, 1}}, {{4, 2}});
    versionTimeline.addTopologyDelta(6, delta4);

    //check values
    //view at time 3
    viewTime = 3;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {3, 4});
    compareNodeAt(versionTimeline, viewTime, 3, {2}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {});

    //view at time 4
    viewTime = 4;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {4});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {3});

    //view at time 6
    viewTime = 6;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2, 4});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {1}, {3});

    //predicting adding of node 5 as child of 1 at time 5
    NES_DEBUG("adding prediction that a new node with id 5 will connect to the system at time 5");
    Experimental::TopologyPrediction::TopologyDelta delta5({{5, 1}}, {});
    versionTimeline.addTopologyDelta(5, delta5);

    NES_DEBUG("node 4 changes prediction that it will connect to node 5 at time 7 instead of to 1 at time 6");
    //changing prediction for node 4 to connect to 5 at time 7 instead of to 1 at time 6 (old prediction should not be visible anymore)
    Experimental::TopologyPrediction::TopologyDelta delta4New({{4, 5}}, {{4, 2}});
    versionTimeline.removeTopologyDelta(6, delta4);
    versionTimeline.addTopologyDelta(7, delta4New);

    //check values
    //view at time 3
    viewTime = 3;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {3, 4});
    compareNodeAt(versionTimeline, viewTime, 3, {2}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {});
    compareNodeAt(versionTimeline, viewTime, 5, {}, {});

    //view at time 4
    viewTime = 4;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {4});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {3});
    compareNodeAt(versionTimeline, viewTime, 5, {}, {});

    //view at time 6
    viewTime = 6;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2, 5});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {4});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {3});
    compareNodeAt(versionTimeline, viewTime, 5, {1}, {});

    //view at time 7
    viewTime = 7;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2, 5});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {5}, {3});
    compareNodeAt(versionTimeline, viewTime, 5, {1}, {4});

    //schedule 2 to connect to 5 at 7, the same time when 4 connects to 5
    NES_DEBUG("schedule 2 to connect to 5 at 7, the same time when 4 connects to 5");
    Experimental::TopologyPrediction::TopologyDelta delta2({{2, 5}}, {{2, 1}});
    versionTimeline.addTopologyDelta(7, delta2);

    //check values
    //view at time 3
    viewTime = 3;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {3, 4});
    compareNodeAt(versionTimeline, viewTime, 3, {2}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {});
    compareNodeAt(versionTimeline, viewTime, 5, {}, {});

    //view at time 4
    viewTime = 4;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {4});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {3});
    compareNodeAt(versionTimeline, viewTime, 5, {}, {});

    //view at time 6
    viewTime = 6;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {2, 5});
    compareNodeAt(versionTimeline, viewTime, 2, {1}, {4});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {2}, {3});
    compareNodeAt(versionTimeline, viewTime, 5, {1}, {});

    //view at time 7
    viewTime = 7;
    compareNodeAt(versionTimeline, viewTime, 1, {}, {5});
    compareNodeAt(versionTimeline, viewTime, 2, {5}, {});
    compareNodeAt(versionTimeline, viewTime, 3, {4}, {});
    compareNodeAt(versionTimeline, viewTime, 4, {5}, {3});
    compareNodeAt(versionTimeline, viewTime, 5, {1}, {4, 2});
}
}// namespace NES
