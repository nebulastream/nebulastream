#include <Topology/Predictions/AggregatedTopologyChangeLog.hpp>
#include <utility>


namespace NES::Experimental {

TopologyPrediction::AggregatedTopologyChangeLog::AggregatedTopologyChangeLog(std::unordered_set<Edge> added,
                                                                             std::unordered_set<Edge> removed)
    : changelogAdded(std::move(added)), changelogRemoved(std::move(removed)) {}

TopologyPrediction::AggregatedTopologyChangeLog::AggregatedTopologyChangeLog(const std::vector<Edge>& added,
                                                                             const std::vector<Edge>& removed) {
    for (auto& addedEdge : added) {
        changelogAdded.insert(addedEdge);
    }
    for (auto& removedEdge : removed) {
        changelogRemoved.insert(removedEdge);
    }
}

void TopologyPrediction::AggregatedTopologyChangeLog::add(const TopologyPrediction::AggregatedTopologyChangeLog& addedChangelog) {
    for (auto& addedEdge : addedChangelog.changelogAdded) {
#ifndef NDEBUG
        if (changelogAdded.contains(addedEdge)) {
            throw std::exception();
        }
#endif
        auto removed = changelogRemoved.find(addedEdge);
        if (removed != changelogRemoved.end()) {
            changelogRemoved.erase(removed);
        } else {
            changelogAdded.insert(addedEdge);
        }
    }
    for (auto& removedEdge : addedChangelog.changelogRemoved) {
#ifndef NDEBUG
        if (changelogRemoved.contains(removedEdge)) {
            throw std::exception();
        }
#endif
        auto added = changelogAdded.find(removedEdge);
        if (added != changelogAdded.end()) {
            changelogAdded.erase(added);
        } else {
            changelogRemoved.insert(removedEdge);
        }
    }
}

const std::unordered_set<TopologyPrediction::Edge>& TopologyPrediction::AggregatedTopologyChangeLog::getAdded() {
    return changelogAdded;
}

const std::unordered_set<TopologyPrediction::Edge>& TopologyPrediction::AggregatedTopologyChangeLog::getRemoved() {
    return changelogRemoved;
}
}