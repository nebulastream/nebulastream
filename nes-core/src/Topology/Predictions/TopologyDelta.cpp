#include <sstream>
#include <Topology/Predictions/TopologyDelta.hpp>

namespace NES::Experimental {

namespace TopologyPrediction {
TopologyDelta::TopologyDelta(std::vector<Edge> added, std::vector<Edge> removed) : added(added), removed(removed) {
  //todo: also make it work without this condition
  for (auto addedIt = added.begin(); addedIt != added.end(); ++addedIt) {
    auto removedIt = std::find(removed.begin(), removed.end(), *addedIt);
    if (removedIt != removed.end()) {
      added.erase(addedIt);
      removed.erase(removedIt);
    }
  }
}

std::string TopologyDelta::toString() {
  std::stringstream deltaString;

  deltaString << "added: {";
  for (auto addedEdge : added) {
    deltaString << addedEdge.toString();
  }
  deltaString << "}, ";

  deltaString << "removed: {";
  for (auto removedEdge : removed) {
    deltaString << removedEdge.toString();
  }
  deltaString << "}, ";

  return deltaString.str();
}
}
}
