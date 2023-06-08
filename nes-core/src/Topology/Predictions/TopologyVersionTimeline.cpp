#include <Topology/Predictions/TopologyVersionTimeline.hpp>
#include <Topology/Predictions/TopologyDelta.hpp>
//todo: predicted node state con probably be omitted if we always reconstruct the whole topology
//#include <PredictedNodeState.hpp>
#include <Topology/Topology.hpp>
//todo: replace this with nes time measurement
//#include <TimeMeasurement.hpp>
#include <Topology/Predictions/TopologyChangeLog.hpp>
//todo: we do not need a topology view if we will completely reconstruct a new topology
//#include <TopologyView.h>
#include <Topology/Predictions/AggregatedTopologyChangeLog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Topology/TopologyNode.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>

//todo: move this into manager?
//todo: use btree by google (use header only lib)
namespace NES::Experimental::TopologyPrediction {
TopologyPtr TopologyVersionTimeline::getTopologyVersion(Timestamp time) {
  //we need to get the node changes with equal or less timestamp
  auto topologyChange = latestChange;
  while (topologyChange && topologyChange->getTime() > time) {
    topologyChange = topologyChange->getPreviousChange();
  }
  //todo: can this be moved up?
  if (!topologyChange) {
    return copyTopology(originalTopology);
  }
  auto nodeChanges = topologyChange->getChangeLog();
  /*
  auto topologyCopy = copyTopology(originalTopology);
  applyAggregatedChangeLog(topologyCopy, nodeChanges);
    return topologyCopy;
   */
  return createTopologyVersion(originalTopology, nodeChanges);
}

TopologyVersionTimeline::TopologyVersionTimeline(TopologyPtr originalTopology)
    :  originalTopology(originalTopology), time(0) {}

Timestamp TopologyVersionTimeline::getTime() {
  return time;
}

[[maybe_unused]] void TopologyVersionTimeline::incrementTime(Timestamp increment) {
  time += increment;
}

void TopologyVersionTimeline::addTopologyChange(Timestamp predictedTime, const TopologyDelta &delta) {
  //todo: garbeage collect also latest change
  if (!latestChange) {
    latestChange = std::make_shared<TopologyChangeLog>(predictedTime);
    latestChange->insert(delta);
    return;
  }
  if (latestChange->getTime() < predictedTime) {
    auto newChange = std::make_shared<TopologyChangeLog>(predictedTime);
    newChange->setPreviousChange(latestChange);
    latestChange = newChange;
    newChange->insert(delta);
    return;
  }
  if (latestChange->getTime() == predictedTime) {
    latestChange->insert(delta);
    return;
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
  newChange->insert(delta);
}

bool TopologyVersionTimeline::removeTopologyChange(Timestamp predictedTime, const TopologyDelta &delta) {
  if (!latestChange) {
    return false;
  }
  if (predictedTime > latestChange->getTime()) {
    return false;
  }
  if (predictedTime == latestChange->getTime()) {
    latestChange->erase(delta);
    //todo: what is going on here with the empty if?
    //if (latestChange->empty());
    if (latestChange->empty()) {
        latestChange = latestChange->getPreviousChange();
    }
    //latestChange = latestChange->getPreviousChange();
    return true;
  }
  auto nextChange = latestChange;
  auto previousChange = nextChange->getPreviousChange();
  while (previousChange && previousChange->getTime() > predictedTime) {
    //todo: garbage collect?
  }
  if (!previousChange || previousChange->getTime() == predictedTime) {
    NES_DEBUG2("Trying to remove prediction at time {} but no topology change was predicted at that time", predictedTime);
    return false;
  }
  previousChange->erase(delta);
  if (previousChange->empty()) {
    nextChange->setPreviousChange(previousChange->getPreviousChange());
  }
  return true;
}

std::string TopologyVersionTimeline::predictionsToString() {
  // indent offset
  int indent = 2;
  std::stringstream predictionsString;
  predictionsString << std::endl;
  predictionsString << "Predictions: " << std::endl;
  for (auto change = latestChange; change; change = change->getPreviousChange()) {
    predictionsString << "Time " << change->getTime() << ": " << std::endl;
    predictionsString << std::string(indent, ' ');
    predictionsString << change->toString() << std::endl;
  }
  return predictionsString.str();
}

//todo: remove
/*
std::string TopologyVersionTimeline::toString(Timestamp viewTime) {
  //we do not allow changing root node
  auto rootId = originalTopology->getRoot()->getId();
  auto topologyView = getTopologyVersion(viewTime);
  auto rootNode = topologyView.findNodeStateWithId(rootId);

  std::stringstream topologyInfo;
  topologyInfo << std::endl;
  topologyInfo << "prediction at timestamp: " << viewTime << std::endl;

  // store pair of TopologyNodePtr and its depth in when printed
  std::deque < std::pair < std::pair < uint64_t, PredictedNodeState >, uint64_t >>
                                                                                parentToPrint{
                                                                                    std::make_pair(std::make_pair(rootId,
                                                                                                                  rootNode),
                                                                                                   0)};

  // indent offset
  int indent = 2;

  // perform dfs traverse
  while (!parentToPrint.empty()) {
    std::pair <std::pair<uint64_t, PredictedNodeState>, uint64_t> nodeToPrint = parentToPrint.front();
    parentToPrint.pop_front();
    for (std::size_t i = 0; i < indent * nodeToPrint.second; i++) {
      if (i % indent == 0) {
        topologyInfo << '|';
      } else {
        if (i >= indent * nodeToPrint.second - 1) {
          topologyInfo << std::string(indent, '-');
        } else {
          topologyInfo << std::string(indent, ' ');
        }
      }
    }
    topologyInfo << nodeToPrint.first.first << std::endl;

    for (const auto &childId : nodeToPrint.first.second.getChildren()) {
      auto child = topologyView.findNodeStateWithId(childId);
      parentToPrint.emplace_front(std::make_pair(childId, child), nodeToPrint.second + 1);
    }
  }
  return topologyInfo.str();
}
 */

//todo: implement
TopologyPtr TopologyVersionTimeline::copyTopology(const TopologyPtr& originalTopology) {
    auto copiedTopology = Topology::create();
    copiedTopology->setAsRoot(originalTopology->getRoot()->copy());
    auto originalIterator = BreadthFirstNodeIterator(originalTopology->getRoot()).begin();

    std::cout << originalTopology->toString() << std::endl;
    std::cout << "------------" << std::endl;
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
    /*
     * todo: this needs to be done differently: iterate over the new originalTopology and add only if:
     * - exists in original and not removed
     * - exists in added
     * case which are guaranteed to not happen:
     * - exists in added and removed
     * - exists in removed but not in original
     */
    //todo: make aggregated originalTopology changelog use maps

    auto copiedTopology = Topology::create();
    copiedTopology->setAsRoot(originalTopology->getRoot()->copy());

    std::queue<TopologyNodePtr> queue;
    queue.push(copiedTopology->getRoot());

    while (!queue.empty()) {
        auto copiedNode = queue.front();
        queue.pop();
        auto nodeId = copiedNode->getId();

        auto originalNode = originalTopology->findNodeWithId(nodeId);
        //std::vector<TopologyNodeId> originalChildren;
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

    /*

    auto originalIterator = BreadthFirstNodeIterator(originalTopology->getRoot()).begin();

    std::cout << originalTopology->toString() << std::endl;
    std::cout << "------------" << std::endl;
    while (originalIterator != NES::BreadthFirstNodeIterator::end()) {
        std::cout << copiedTopology->toString() << std::endl;
        auto originalNode = (*originalIterator)->as<TopologyNode>();
        auto copyNode = copiedTopology->findNodeWithId(originalNode->getId());

        //check if the node exists in the changed originalTopology
        if (!copyNode) {
            continue;
        }

        for (auto& originalChild : originalNode->getChildren()) {
            //todo: this is not very efficient anymore now that we have to construct a whole new originalTopology
            //todo: continue with out adding if the edge has been removed
            //if (std::find(changeLog.getRemoved().begin(), changeLog.getRemoved().end(), {originalChild->}))
            auto copiedChild = copiedTopology->findNodeWithId(originalChild->as<TopologyNode>()->getId());

            //check if the node was already added as the child of another node
            if (copiedChild) {
                NES_DEBUG2("Adding node {} as additional parent of existing node {}", copyNode->getId(), copiedChild->getId());
            } else {
                //create new copy of the node because it was not found in the copied originalTopology
                copiedChild = originalChild->as<TopologyNode>()->copy();
                NES_DEBUG2("Adding new node {} as child of {}", copiedChild->getId(), copyNode->getId());
            }
            copiedTopology->addNewTopologyNodeAsChild(copyNode, copiedChild);
        }


        ++originalIterator;
    }
     */
    return copiedTopology;
}

//todo: check in both cases if parent and child exist, in first case create them if needed
//todo: probably not needed anymore now that we have the new function`
/*
void TopologyVersionTimeline::applyAggregatedChangeLog(TopologyPtr topology, AggregatedTopologyChangeLog changeLog) {
    for (auto &addedEdge : changeLog.getAdded()) {
        //todp: check that nodes exist?
        //topology->findNodeWithId(addedEdge.parent)->addChild(topology->findNodeWithId(addedEdge.child));
        auto parent = topology->findNodeWithId(addedEdge.parent);
        if (!parent) {

        }
        auto child = topology->findNodeWithId(addedEdge.child);
        topology->addNewTopologyNodeAsChild(parent, child);
    }
    for (auto &removedEdge : changeLog.getRemoved()) {
        auto child = topology->findNodeWithId(removedEdge.child);
        if (!child) {
            NES_DEBUG2("Node with id {} not found, possibly because its only parents was removed", removedEdge.child);
            continue;
        }
        topology->findNodeWithId(removedEdge.parent)->removeChild(child);
        if (child->getParents().empty()) {
            topology->removePhysicalNode(child);
        }
        //todo: do we nned to remove nodes without parents here?
    }
}
 */
}