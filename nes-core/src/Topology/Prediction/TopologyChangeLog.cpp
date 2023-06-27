#include <Topology/Prediction/TopologyChangeLog.hpp>
#include <Topology/Prediction/TopologyDelta.hpp>

namespace NES::Experimental::TopologyPrediction {

void TopologyChangeLog::addMap(const std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>>& newMap,
            std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>>& additionTarget,
            std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>>& toSubtract) {
    //iterate over the parent ids and corresponding lists of children
    for (const auto& [parentToAdd, childrenToAdd] : newMap) {
        //get the list of children to which new children will be added
        auto& additionTargetChildren = additionTarget[parentToAdd];
        //get the children which are not to be added even if they appead in the list of children to add
        auto& childrenToSubtract = toSubtract[parentToAdd];
        for (const auto childToAdd : childrenToAdd) {
            auto childToSubtract = std::find(childrenToSubtract.begin(), childrenToSubtract.end(), childToAdd);
            if (childToSubtract != childrenToSubtract.end()) {
                //if the child that is to be added, is also subtracted, remove it from the subtraction list and do not add it to the addition target
                childrenToSubtract.erase(childToSubtract);
                continue;
            }

#ifndef NDEBUG
            //if the edge to be added is also added as part of another changelog, this means our data is corrupted
            if (std::find(additionTargetChildren.begin(), additionTargetChildren.end(), childToAdd)
                != additionTargetChildren.end()) {
                throw std::exception();
            }
#endif
            additionTargetChildren.push_back(childToAdd);
        }
        //remove empty vectors in target
        if (additionTargetChildren.empty()) {
            additionTarget.erase(parentToAdd);
        }
        if (childrenToSubtract.empty()) {
            toSubtract.erase(parentToAdd);
        }
    }
}

void TopologyChangeLog::add(const TopologyChangeLog& addedChangelog) {
    addMap(addedChangelog.changelogAdded, changelogAdded, changelogRemoved);
    addMap(addedChangelog.changelogRemoved, changelogRemoved, changelogAdded);
}

std::vector<TopologyNodeId> TopologyChangeLog::getAddedChildren(TopologyNodeId nodeId) {
    auto it = changelogAdded.find(nodeId);
    if (it != changelogAdded.end()) {
        return it->second;
    }
    return {};
}

std::vector<TopologyNodeId> TopologyChangeLog::getRemovedChildren(TopologyNodeId nodeId) {
    auto it = changelogRemoved.find(nodeId);
    if (it != changelogRemoved.end()) {
        return it->second;
    }
    return {};
}

void TopologyChangeLog::insert(const TopologyDelta& newDelta) {
    for (auto addedEdge : newDelta.getAdded()) {
        changelogAdded[addedEdge.parent].push_back(addedEdge.child);
    }
    for (auto removedEdge : newDelta.getRemoved()) {
        changelogRemoved[removedEdge.parent].push_back(removedEdge.child);
    }
}

void TopologyChangeLog::erase(const TopologyDelta& delta) {
    removeEdgesFromMap(changelogAdded, delta.getAdded());
    removeEdgesFromMap(changelogRemoved, delta.getRemoved());
}

void TopologyChangeLog::removeEdgesFromMap(std::unordered_map<TopologyNodeId, std::vector<TopologyNodeId>>& map,
    const std::vector<Edge>& edges) {
    for (auto edge : edges) {
        auto& children = map[edge.parent];
        auto iterator = std::find(children.begin(), children.end(), edge.child);
        children.erase(iterator);
        if (children.empty()) {
            map.erase(edge.parent);
        }
    }
}

bool TopologyChangeLog::empty() {
    return changelogAdded.empty() && changelogRemoved.empty();
}
}