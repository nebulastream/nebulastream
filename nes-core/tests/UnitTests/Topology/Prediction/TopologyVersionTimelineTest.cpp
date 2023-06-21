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

#include <Configurations/WorkerConfigurationKeys.hpp>
#include <NesBaseTest.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Topology/Predictions/TopologyDelta.hpp>
#include <Topology/Predictions/TopologyVersionTimeline.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace NES {

class TopologyVersionTimelineTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {

        NES::Logger::setupLogging("TopologyVersionTimelineTest.log", NES::LogLevel::LOG_NONE);
        NES_DEBUG("Setup TopologyVersionTimeline test class.");
    }

    static std::vector<TopologyNodeId> getIdVector(const std::vector<NodePtr>& nodes) {
        std::vector<TopologyNodeId> ids;
        ids.reserve(nodes.size());
        for (auto& node : nodes) {
            ids.push_back(node->as<TopologyNode>()->getId());
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

    static void compareNodeAt(Experimental::TopologyPrediction::TopologyVersionTimeline versions, Timestamp time, uint64_t id, std::vector<uint64_t> parents, std::vector<uint64_t> children) {
        std::cout << "compare node " << id << std::endl;
        auto predictedNode = versions.getTopologyVersion(time)->findNodeWithId(id);

        //check if the node is predicted to be removed
        if (children.empty() && parents.empty()) {
            EXPECT_EQ(predictedNode, nullptr);
            return ;
        }

        //compare links
        std::sort(children.begin(), children.end());
        std::sort(parents.begin(), parents.end());

        EXPECT_EQ(getIdVector(predictedNode->getChildren()), children);
        EXPECT_EQ(getIdVector(predictedNode->getParents()), parents);
    }

  protected:
    TopologyNodePtr rootNode, mid1, mid2, mid3, src1, src2, src3, src4;
};

TEST_F(TopologyVersionTimelineTest, testUpdatingMultiplePredictions) {
    auto topology = Topology::create();
    std::map<std::string, std::any> properties;
    topology->setAsRoot(TopologyNode::create(1, "localhost", 4001, 5001, 4, properties));
    topology->addNewTopologyNodeAsChild(topology->findNodeWithId(1), TopologyNode::create(2, "localhost", 4001, 5001, 4, properties));
    topology->addNewTopologyNodeAsChild(topology->findNodeWithId(2), TopologyNode::create(3, "localhost", 4001, 5001, 4, properties));
    topology->addNewTopologyNodeAsChild(topology->findNodeWithId(2), TopologyNode::create(4, "localhost", 4001, 5001, 4, properties));
    std::cout << "Original Topology" << std::endl;
    std::cout << topology->toString() << std::endl;
    Experimental::TopologyPrediction::TopologyVersionTimeline versionTimeline(topology);

    //create new prediction for node 3 to reconnect to node 4
    std::cout << "adding new prediction that node 3 will reconnect to node 4 at time 7" << std::endl;
    Experimental::TopologyPrediction::TopologyDelta delta3({{3, 4}}, {{3, 2}});
    versionTimeline.addTopologyDelta(7, delta3);

    Timestamp viewTime;
    std::cout << versionTimeline.predictionsToString() << std::endl;

    std::cout << "views: " << std::endl;
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
    std::cout << "updating prediction that node 3 will reconnect to node 4 at time 4 instead of time 7" << std::endl;
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
    std::cout << "adding new prediction that node 4 will reconnect to node 1 at time 6" << std::endl;
    Experimental::TopologyPrediction::TopologyDelta delta4({{4, 1}}, {{4, 2}});
    versionTimeline.addTopologyDelta(6, delta4);

    std::cout << versionTimeline.predictionsToString() << std::endl;
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
    std::cout << "adding prediction that a new node with id 5 will connect to the system at time 5" << std::endl;
    Experimental::TopologyPrediction::TopologyDelta delta5({{5, 1}}, {});
    versionTimeline.addTopologyDelta(5, delta5);

    std::cout << "node 4 changes prediction that it will connect to node 5 at time 7 instead of to 1 at time 6" << std::endl;
    //changing predciction for node 4 to connect to 5 at time 7 instead of to 1 at time 6 (old prediction should not be visible anymore)
    Experimental::TopologyPrediction::TopologyDelta delta4New({{4, 5}}, {{4, 2}});
    versionTimeline.removeTopologyDelta(6, delta4);
    versionTimeline.addTopologyDelta(7, delta4New);

    std::cout << versionTimeline.predictionsToString() << std::endl;
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
    std::cout << std::endl;
    std::cout << "schedule 2 to connect to 5 at 7, the same time when 4 connects to 5" << std::endl;
    Experimental::TopologyPrediction::TopologyDelta delta2({{2, 5}}, {{2, 1}});
    versionTimeline.addTopologyDelta(7, delta2);

    std::cout << versionTimeline.predictionsToString() << std::endl;
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
