#include <Topology/Predictions/AggregatedTopologyChangeLog.hpp>
#include <utility>


namespace NES::Experimental {

TopologyPrediction::AggregatedTopologyChangeLog::AggregatedTopologyChangeLog(std::unordered_set<Edge> added,
                                                                             std::unordered_set<Edge> removed){
    for (auto& addedEdge : added) {
        if (removed.contains(addedEdge)) {
            removed.erase(addedEdge);
            continue;
        }
        changelogAdded[addedEdge.parent].push_back(addedEdge.child);
    }

    for (auto& removedEdge : removed) {
        //because of the check in the loop above, we already know, that no removed edge is also in the added edge set
        changelogRemoved[removedEdge.parent].push_back(removedEdge.child);
    }
}

TopologyPrediction::AggregatedTopologyChangeLog::AggregatedTopologyChangeLog(const std::vector<Edge>& added,
                                                                             const std::vector<Edge>& removed) {
    for (auto& addedEdge : added) {
        //changelogAdded.insert(addedEdge);
        changelogAdded[addedEdge.parent].push_back(addedEdge.child);
    }
    for (auto& removedEdge : removed) {
        changelogRemoved[removedEdge.parent].push_back(removedEdge.child);
    }
}

void addMap(const std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>>& newMap, std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>>& additionTarget, std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>>& toSubtract) {
    for (auto& [parentToAdd, childrenToAdd] : newMap)  {
        auto& additionTargetChildren = additionTarget[parentToAdd];
        auto& childrenToSubstract = toSubtract[parentToAdd];
        for (auto childToAdd : childrenToAdd) {
            auto childToSubstract = std::find(childrenToSubstract.begin(), childrenToSubstract.end(), childToAdd);
            if (childToSubstract != childrenToSubstract.end()) {
                //if the child that is to be added, is also subtracted, remove it from the subtraction list and do not added to the addition target
                childrenToSubstract.erase(childToSubstract);
                continue;
            }

#ifndef NDEBUG
            //if the edge to be added is also added as part of another changelog, this means our data is corrupted
            if (std::find(additionTargetChildren.begin(), additionTargetChildren.end(), childToAdd) != additionTargetChildren.end()) {
                throw std::exception();
            }
#endif
            additionTargetChildren.push_back(childToAdd);
        }
        //remove empty vectors in target
        if (additionTargetChildren.empty()) {
            additionTarget.erase(parentToAdd);
        }
        if (childrenToSubstract.empty()) {
            toSubtract.erase(parentToAdd);
        }
    }
}

void TopologyPrediction::AggregatedTopologyChangeLog::add(const TopologyPrediction::AggregatedTopologyChangeLog& addedChangelog) {
    //todo: does it really work this way?
    addMap(addedChangelog.changelogAdded, changelogAdded, changelogRemoved);
    addMap(addedChangelog.changelogRemoved, changelogRemoved, changelogAdded);
    /*
    for (auto& edgesToAdd : addedChangelog.changelogAdded) {
        auto addedChildren = changelogAdded[edgesToAdd.first];
        auto removedChildren = changelogAdded[edgesToAdd.first];
        for (auto& child : edgesToAdd.second) {
            //check if the added child was removed before, if so, remove and addition cancel each other out
            auto removed = std::find(removedChildren.begin(), removedChildren.end(), child);
            if (removed != removedChildren.end()) {
                removedChildren.erase(removed);
                continue;
            }

#ifndef NDEBUG
            if (std::find(addedChildren.begin(), addedChildren.end(), child) != addedChildren.end()) {
                throw std::exception();
            }
#endif

            //if the child was not removed by another changelog, put it to the added list
            addedChildren.push_back(child);

            //todo remove if empty
        }


#ifndef NDEBUG
        //if (changelogAdded.contains(addedEdge)) {
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
*/
}

/*
const std::unordered_set<TopologyPrediction::Edge>& TopologyPrediction::AggregatedTopologyChangeLog::getAdded() {
    return changelogAdded;
}

const std::unordered_set<TopologyPrediction::Edge>& TopologyPrediction::AggregatedTopologyChangeLog::getRemoved() {
    return changelogRemoved;
}
 */

std::vector<TopologyNodeId> TopologyPrediction::AggregatedTopologyChangeLog::getAddedChildren(TopologyNodeId nodeId) {
    auto it = changelogAdded.find(nodeId);
    if (it != changelogAdded.end()) {
        return it->second;
    }
    return {};
}

std::vector<TopologyNodeId> TopologyPrediction::AggregatedTopologyChangeLog::getRemoveChildren(TopologyNodeId nodeId) {
    auto it = changelogRemoved.find(nodeId);
    if (it != changelogRemoved.end()) {
        return it->second;
    }
    return {};
}
}