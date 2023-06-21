#include <sstream>
#include <Topology/Predictions/TopologyChangeLog.hpp>
#include <Topology/Predictions/TopologyDelta.hpp>
#include <Topology/Predictions/AggregatedTopologyChangeLog.hpp>
namespace NES::Experimental {

namespace TopologyPrediction {
void TopologyChangeLog::insert(const TopologyDelta &newDelta) {
  //todo: add error handling here to prevent edges being added multiple times/
  //todo: this will break if we have the same edge being added and removed and added again in the same step
  for (auto addedEdge : newDelta.added) {
    changelogAdded.insert(addedEdge);
  }
  for (auto removedEdge : newDelta.removed) {
    changelogRemoved.insert(removedEdge);
  }
}

void TopologyChangeLog::erase(const TopologyDelta &delta) {
  for (auto addedEdge : delta.added) {
    changelogAdded.erase(addedEdge);
  }
  for (auto removedEdge : delta.removed) {
    changelogRemoved.erase(removedEdge);
  }
}

std::string TopologyChangeLog::toString() {
  std::stringstream predictionsString;
  predictionsString << "added: [";
  for (auto e : changelogAdded) {
    predictionsString << e.toString() << ", ";
  }
  predictionsString << "] ";
  predictionsString << "removed: [";
  for (auto e : changelogRemoved) {
    predictionsString << e.toString() << ", ";
  }
  predictionsString << "]";

  return predictionsString.str();
}

//todo: make an optimized version of this, that does not pass on a node change if it does not meet the requierment of a certain request time
AggregatedTopologyChangeLog TopologyChangeLog::getChangeLog() {
  AggregatedTopologyChangeLog delta(changelogAdded, changelogRemoved);
  if (previousChange) {
    auto previousChangeLog = previousChange->getChangeLog();
    previousChangeLog.add(delta);
    return previousChangeLog;
  }
  return delta;
}

bool TopologyChangeLog::empty() {
  return changelogAdded.empty() && changelogRemoved.empty();
}

Timestamp TopologyChangeLog::getTime() {
  return time;
}

TopologyChangeLog::TopologyChangeLog(Timestamp time) : time(time) {}
}
}