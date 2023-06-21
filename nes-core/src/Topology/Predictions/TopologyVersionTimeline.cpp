#include <Topology/Predictions/TopologyVersionTimeline.hpp>
#include <Topology/Predictions/TopologyDelta.hpp>
#include <Topology/Topology.hpp>
#include <Topology/Predictions/TopologyChangeLog.hpp>
#include <Topology/Predictions/AggregatedTopologyChangeLog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Topology/TopologyNode.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>

//todo: move this into manager?
//todo: use btree by google (use header only lib)
namespace NES::Experimental::TopologyPrediction {
TopologyPtr TopologyVersionTimeline::getTopologyVersion(Timestamp time) {
  //we need to get the node changes with equal or less timestamp
  //auto topologyChange = getTopologyChangeLogBefore(time);
  //auto topologyChange = changeMap.;

  //todo: do we still need to handle this case extra
  /*
  if (!topologyChange) {
    return copyTopology(originalTopology);
  }
   */

  //auto nodeChanges = topologyChange->getChangeLog();
  auto nodeChanges = createAggregatedChangeLog(time);

  /*
  auto topologyCopy = copyTopology(originalTopology);
  applyAggregatedChangeLog(topologyCopy, nodeChanges);
    return topologyCopy;
   */
  return createTopologyVersion(originalTopology, nodeChanges);
}

//todo: make references out of these
std::optional<AggregatedTopologyChangeLog> TopologyVersionTimeline::getTopologyChangeLogBefore(Timestamp time) {
  //get the first element that has a greater timestamp than the one we are looking for and take the one before
  auto iterator = changeMap.upper_bound(time);
  if (iterator != changeMap.begin()) {
    --iterator;
    return iterator->second;
  }
  return std::nullopt;
  /*
    auto topologyChange = latestChange;
    while (topologyChange && topologyChange->getTime() > time) {
        topologyChange = topologyChange->getPreviousChange();
    }
    return topologyChange;
    */
}

//std::optional<AggregatedTopologyChangeLog&> TopologyVersionTimeline::getTopologyChangeLogAt(Timestamp time) {
    /*
    if (!latestChange) {
        return false;
    }
    if (predictedTime > latestChange->getTime()) {
        return false;
    }
     */

  /*
    auto changeLog = latestChange;
    while (changeLog != nullptr && changeLog->getTime() >= time) {
        changeLog = changeLog->getPreviousChange();
    }
    if (changeLog->getTime() != time) {
        return nullptr;
    }
    return changeLog;
    */


  //todo: fix and reactivete the following block
  /*
    auto iterator = changeMap.find(time);
    if (iterator == changeMap.end()) {
    return std::nullopt;
    }
    return iterator->second;
    */

  /*
  if (changeMap.contains(time)) {
    return changeMap.at(time);
  }
  return std::nullopt;
   */
//}

bool TopologyVersionTimeline::removeTopologyChangeLogAt(Timestamp time) {
    //auto changeLog = getTopologyChangeLogAt(time);
    return changeMap.erase(time);
}

//TopologyChangeLogPtr TopologyVersionTimeline::getOrCreateTopologyChangeLogAt(Timestamp predictedTime) {
    /*
    if (!latestChange) {
        latestChange = std::make_shared<TopologyChangeLog>(predictedTime);
        //latestChange->insert(delta);
        return latestChange;
    }
    if (latestChange->getTime() < predictedTime) {
        auto newChange = std::make_shared<TopologyChangeLog>(predictedTime);
        newChange->setPreviousChange(latestChange);
        latestChange = newChange;
        //newChange->insert(delta);
        return newChange;
    }
    if (latestChange->getTime() == predictedTime) {
        //latestChange->insert(delta);
        return latestChange;
    }
    auto nextChange = latestChange;
    auto previousChange = nextChange->getPreviousChange();
    while (previousChange && previousChange->getTime() > predictedTime) {

        //todo: garbage collect?
        nextChange = nextChange->getPreviousChange();
        previousChange = nextChange->getPreviousChange();
    }

    auto newChange = std::make_shared<TopologyChangeLog>(predictedTime);
    if (!previousChange) {
        newChange = std::make_shared<TopologyChangeLog>(predictedTime);
        nextChange->setPreviousChange(newChange);
    } else if (previousChange->getTime() == predictedTime) {
        nextChange = previousChange;
    } else {
        newChange = std::make_shared<TopologyChangeLog>(predictedTime);
        nextChange->setPreviousChange(newChange);
        newChange->setPreviousChange(previousChange);
    }
    return newChange;
     */
   // return changeMap[predictedTime];
//}


TopologyVersionTimeline::TopologyVersionTimeline(TopologyPtr originalTopology)
    :  originalTopology(originalTopology) {}

void TopologyVersionTimeline::addTopologyDelta(Timestamp predictedTime, const TopologyDelta &delta) {
  //todo: garbeage collect also latest change
    //auto newChange = getOrCreateTopologyChangeLogAt(predictedTime);
  //newChange->insert(delta);
    auto& newChange = changeMap[predictedTime];
    /*
    if (!newChange) {
    //todo: remove time from the constructor of the changelog
    //todo: when we make this not contain pointers, this will also be way more elegant
        newChange = std::make_shared<TopologyChangeLog>(predictedTime);
    }
     */
    newChange.insert(delta);
}

bool TopologyVersionTimeline::removeTopologyDelta(Timestamp predictedTime, const TopologyDelta &delta) {
    /*
  auto changeLog = getTopologyChangeLogAt(predictedTime);
  if (!changeLog) {
        return false;
  }
     */
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

std::string TopologyVersionTimeline::predictionsToString() {
  // indent offset
  int indent = 2;
  std::stringstream predictionsString;
  predictionsString << std::endl;
  predictionsString << "Predictions: " << std::endl;
  //todo: reenable with map
  /*
  for (auto change = latestChange; change; change = change->getPreviousChange()) {
    predictionsString << "Time " << change->getTime() << ": " << std::endl;
    predictionsString << std::string(indent, ' ');
    predictionsString << change->toString() << std::endl;
  }
   */
  return predictionsString.str();
}

TopologyPtr TopologyVersionTimeline::copyTopology(const TopologyPtr& originalTopology) {
    auto copiedTopology = Topology::create();
    copiedTopology->setAsRoot(originalTopology->getRoot()->copy());
    auto originalIterator = BreadthFirstNodeIterator(originalTopology->getRoot()).begin();

    while (originalIterator != NES::BreadthFirstNodeIterator::end()) {
        std::cout << copiedTopology->toString() << std::endl;
        auto originalNode = (*originalIterator)->as<TopologyNode>();
        auto copyNode = copiedTopology->findNodeWithId(originalNode->getId());
        for (auto& originalChild : originalNode->getChildren()) {
            auto copiedChild = copiedTopology->findNodeWithId(originalChild->as<TopologyNode>()->getId());

            //check if the node was already added as the child of another node
            if (copiedChild) {
                NES_DEBUG2("Adding node {} as additional parent of existing node {}", copyNode->getId(), copiedChild->getId());
            } else {
                //create new copy of the node because it was not found in the copied topology
                copiedChild = originalChild->as<TopologyNode>()->copy();
                NES_DEBUG2("Adding new node {} as child of {}", copiedChild->getId(), copyNode->getId());
            }
            copiedTopology->addNewTopologyNodeAsChild(copyNode, copiedChild);
        }
        ++originalIterator;
    }
    return copiedTopology;
}

TopologyPtr TopologyVersionTimeline::createTopologyVersion(TopologyPtr originalTopology, AggregatedTopologyChangeLog changeLog) {
    auto copiedTopology = Topology::create();
    copiedTopology->setAsRoot(originalTopology->getRoot()->copy());

    std::queue<TopologyNodePtr> queue;
    queue.push(copiedTopology->getRoot());

    while (!queue.empty()) {
        auto copiedNode = queue.front();
        queue.pop();
        auto nodeId = copiedNode->getId();

        auto originalNode = originalTopology->findNodeWithId(nodeId);
        if (originalNode) {
            auto removedChildren = changeLog.getRemoveChildren(nodeId);
            for (auto& originalChild : originalNode->getChildren()) {
                auto childId = originalChild->as<TopologyNode>()->getId();
                //cheeck if hte node has already been added
                //check that the edge was not removed
                if (std::find(removedChildren.begin(), removedChildren.end(), childId) == removedChildren.end()) {
                    //if the edge is not marked as removed add a copy of the child to the new topology
                    auto copiedChild = copiedTopology->findNodeWithId(childId);
                    if (!copiedChild) {
                        copiedChild = originalChild->as<TopologyNode>()->copy();
                        queue.push(copiedChild);
                    }
                    NES_DEBUG2("adding edge based on copy from {} to {}", nodeId, childId);
                    copiedTopology->addNewTopologyNodeAsChild(copiedNode, copiedChild);
                }
            }
        } else {
            //if no original node exists, no removed edges should exist
#ifndef NDEBUG
            if (!changeLog.getRemoveChildren(nodeId).empty()) {
                throw std::exception();
            }
#endif
        }

        for (auto& addedChild : changeLog.getAddedChildren(nodeId)) {
            //todo: check firs if the ndoe already exists in the system, then no new one has to be created
            auto childNode = copiedTopology->findNodeWithId(addedChild);
            if (!childNode) {
                //todo: make separate data structure which records node data
                auto originalChildNode = originalTopology->findNodeWithId(addedChild);
                if (!originalChildNode) {
                    std::map<std::string, std::any> properties;
                    //todo: ad newly created nodes here with a dummy configuration
                    //todo: from which data structure should we later get the actual data?
                    childNode = TopologyNode::create(addedChild, "localhost", 4001, 5001, 4, properties);
                } else {
                    childNode = originalChildNode->copy();
                }
                queue.push(childNode);
            }
            NES_DEBUG2("adding edge based on prediction from {} to {}", nodeId, childNode->getId());
            copiedTopology->addNewTopologyNodeAsChild(copiedNode, childNode);
        }
    }
    return copiedTopology;
}
AggregatedTopologyChangeLog TopologyVersionTimeline::createAggregatedChangeLog(Timestamp time) {
     AggregatedTopologyChangeLog aggregatedTopologyChangeLog;
     for (auto changeLog = changeMap.begin(); changeLog != changeMap.end() && changeLog->first <= time; ++changeLog) {
        //todo: this can be optimized to use less calls
        aggregatedTopologyChangeLog.add(changeLog->second);
     }
     return aggregatedTopologyChangeLog;
}
}