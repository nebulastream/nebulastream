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
#include <Catalogs/Topology/Prediction/TopologyChangeLog.hpp>
#include <Catalogs/Topology/Prediction/TopologyDelta.hpp>
#include <Catalogs/Topology/Prediction/TopologyTimeline.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Experimental::TopologyPrediction {
TopologyTimeline::TopologyTimeline(TopologyPtr originalTopology) : originalTopology(std::move(originalTopology)) {}

TopologyTimelinePtr TopologyTimeline::create(TopologyPtr originalTopology) {
    return std::make_shared<TopologyTimeline>(originalTopology);
}

TopologyPtr TopologyTimeline::getTopologyVersion(Timestamp time) {
    //to get the node changes with timestamp equal or less than time
    auto nodeChanges = createAggregatedChangeLog(time);
    return createTopologyVersion(nodeChanges);
}

void TopologyTimeline::removeTopologyChangeLogAt(Timestamp time) { changeMap.erase(time); }

void TopologyTimeline::addTopologyDelta(Timestamp predictedTime, const TopologyDelta& delta) {
    auto& change = changeMap[predictedTime];
    change.update(delta);
}

bool TopologyTimeline::removeTopologyDelta(Timestamp predictedTime, const TopologyDelta& delta) {
    if (!changeMap.contains(predictedTime)) {
        return false;
    }
    auto& changeLog = changeMap[predictedTime];
    changeLog.erase(delta);
    if (changeLog.empty()) {
        removeTopologyChangeLogAt(predictedTime);
    }
    return true;
}

TopologyPtr TopologyTimeline::createTopologyVersion(const TopologyChangeLog& changeLog) {
    auto copiedTopology = Topology::create();
    copiedTopology->setRootTopologyNodeId(originalTopology->getRoot()->copy());

    //bfs starting at root node
    std::queue<TopologyNodePtr> queue;
    queue.push(copiedTopology->getRoot());

    while (!queue.empty()) {
        auto copiedNode = queue.front();
        queue.pop();
        auto nodeId = copiedNode->getId();

        auto originalNode = originalTopology->findWorkerWithId(nodeId);
        if (originalNode) {
            /*if the node exists in the original topology, add iterate over its children and add them to the copy if they are not
            listed as removed by in the changelog*/
            auto removedChildren = changeLog.getRemovedChildren(nodeId);
            for (auto& originalChild : originalNode->getChildren()) {
                auto childId = originalChild->as<TopologyNode>()->getId();
                //check if the edge is listed as removed in the changelog
                if (std::find(removedChildren.begin(), removedChildren.end(), childId) == removedChildren.end()) {
                    //if the edge is not marked as removed in the changelog, add a copy of the child to the new topology
                    auto copiedChild = copiedTopology->findWorkerWithId(childId);
                    if (!copiedChild) {
                        copiedChild = originalChild->as<TopologyNode>()->copy();
                        queue.push(copiedChild);
                    }
                    NES_DEBUG("adding edge based on copy from {} to {}", nodeId, childId);
                    copiedTopology->addNewTopologyNodeAsChild(copiedNode, copiedChild);
                }
            }
        } else {
#ifndef NDEBUG
            //if no original node exists, no removed edges should exist
            if (!changeLog.getRemovedChildren(nodeId).empty()) {
                throw std::exception();
            }
#endif
        }

        //iterate over the added children listed in the changelog
        for (auto& addedChild : changeLog.getAddedChildren(nodeId)) {
            auto childNode = copiedTopology->findWorkerWithId(addedChild);
            //check first if the child node already exists in the copied topology. otherwise a new one has to be created
            if (!childNode) {
                auto originalChildNode = originalTopology->findWorkerWithId(addedChild);
                //check if the node exists in the original topology
                if (!originalChildNode) {
                    //if the node does neither exist in the original not in the copy, it has to be created
                    std::map<std::string, std::any> properties;
                    //todo 3938: we are adding newly created nodes here with a dummy configuration. from which data structure should we get the actual data?
                    childNode = TopologyNode::create(addedChild, "localhost", 4001, 5001, 4, properties);
                } else {
                    //if the node exists in the original but not in the copy, it can be copied from the original
                    childNode = originalChildNode->copy();
                }
                //queue the newly created node to be iterated over
                queue.push(childNode);
            }
            NES_DEBUG("adding edge based on prediction from {} to {}", nodeId, childNode->getId());
            //add a link between parent and child
            copiedTopology->addNewTopologyNodeAsChild(copiedNode, childNode);
        }
    }
    return copiedTopology;
}

TopologyChangeLog TopologyTimeline::createAggregatedChangeLog(Timestamp time) {
    TopologyChangeLog aggregatedTopologyChangeLog;
    //todo #3937: garbage collect
    //todo: create issue for caching
    for (auto changeLog = changeMap.begin(); changeLog != changeMap.end() && changeLog->first <= time; ++changeLog) {
        aggregatedTopologyChangeLog.add(changeLog->second);
    }
    return aggregatedTopologyChangeLog;
}
}// namespace NES::Experimental::TopologyPrediction