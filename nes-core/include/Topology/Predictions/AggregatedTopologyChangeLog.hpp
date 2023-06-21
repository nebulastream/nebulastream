#ifndef TOPOLOGYPREDICTION_CHANGELOG_H
#define TOPOLOGYPREDICTION_CHANGELOG_H

#include "Common/Identifiers.hpp"
#include <Topology/Predictions/Edge.hpp>
#include <unordered_set>
#include <Topology/Predictions/TopologyDelta.hpp>
namespace NES::Experimental {

namespace TopologyPrediction {
class TopologyChangeLog;

/**
 * This class represents a list of changes which are expected to happen to the topology until a certain point in time
 * The changelog for time t includes all changes expected to occur before t as well
 */
class AggregatedTopologyChangeLog {
  public:
    AggregatedTopologyChangeLog() = default;

    AggregatedTopologyChangeLog(std::unordered_set<Edge> added, std::unordered_set<Edge> removed);

    AggregatedTopologyChangeLog(const std::vector<Edge>& added, const std::vector<Edge>& removed);

    void add(const AggregatedTopologyChangeLog& addedChangelog);

    //const std::unordered_set<Edge>& getAdded();

    //const std::unordered_set<Edge>& getRemoved();

    void insert(const TopologyDelta& newDelta);

    void erase(const TopologyDelta& delta);

    bool empty();

    std::vector<TopologyNodeId> getAddedChildren(TopologyNodeId nodeId);
    std::vector<TopologyNodeId> getRemoveChildren(TopologyNodeId nodeId);
    void removeEdgesFromMap(std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>>& map, const std::vector<Edge> edges);
  private:
    //std::unordered_set<Edge> changelogAdded;
    //std::unordered_set<Edge> changelogRemoved;
    std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>> changelogAdded;
    std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>> changelogRemoved;
};
}
}
#endif //TOPOLOGYPREDICTION_CHANGELOG_H
