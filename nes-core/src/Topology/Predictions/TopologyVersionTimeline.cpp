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
  if (!topologyChange) {
    return copyTopology(originalTopology);
  }
  auto nodeChanges = topologyChange->getChangeLog();
  auto topologyCopy = copyTopology(originalTopology);
  applyAggregatedChangeLog(originalTopology, nodeChanges);
  return originalTopology;
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
    if (latestChange->empty());
    latestChange = latestChange->getPreviousChange();
    return true;
  }
  auto nextChange = latestChange;
  auto previousChange = nextChange->getPreviousChange();
  while (previousChange && previousChange->getTime() > predictedTime) {
    //todo: garbage collect?
  }
  if (!previousChange || previousChange->getTime() == predictedTime) {
    //todo: reactivate after setting log level
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

//todo:implement
void TopologyVersionTimeline::applyAggregatedChangeLog(TopologyPtr topology, AggregatedTopologyChangeLog changeLog) {
  (void) topology;
  (void) changeLog;

}

}