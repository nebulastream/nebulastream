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
#ifndef NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_PREDICTION_TOPOLOGYCHANGELOG_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_PREDICTION_TOPOLOGYCHANGELOG_HPP_

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace NES {

using WorkerId = uint64_t;
class TopologyLinkInformation;

namespace Experimental::TopologyPrediction {
class TopologyDelta;
/**
     * This class represents a list of changes (added or removed links) which are expected to happen to the topology within a
     * certain time frame. Predictions can be added or removed from a changelog in the form of TopologyDeltas.
     */
class TopologyChangeLog {
  public:
    /**
         * @brief construct an empty changelog
         */
    TopologyChangeLog() = default;

    /**
         * @brief add another changelog to this one. If an edge is removed in one changelog and added in the other, it will neither
         * appear as added nor as removed in the resulting changelog.
         * @param newChangeLog the changelog whose contents are to be added to this changelog. The caller has to ensure that the
         * added changelog does not contain any added edges which are already present as added or any removed edges which are
         * already present as removed edges
         */
    void add(const TopologyChangeLog& newChangeLog);

    /**
         * @brief insert information about added and removed edges from a topology delta to this changelog
         * @param newDelta the delta whose information is to be inserted into this changelog. The caller has to ensure that the added
         * delta does not contain any added edges which are already present as added in the changelog or any removed edges which are
         * already present as removed edges
         */
    void update(const TopologyDelta& newDelta);

    /**
         * @brief remove information about added and removed edges from a topology delta from this changelog
         * @param newDelta the delta whose information is to be removed from this changelog
         */
    void erase(const TopologyDelta& delta);

    /**
         * @brief indicates if this changelog is empty
         * @return true if this changelog does not contain any added or removed links
         */
    bool empty();

    /**
         * @brief get a list of the children which would be added to a certain node if the changes in this changelog were applied
         * @param nodeId the id of the node in question
         * @return a vector of node ids
         */
    std::vector<WorkerId> getAddedChildren(WorkerId nodeId) const;

    /**
         * @brief get a list of the children which would be removed from a certain node if the changes in this changelog were applied
         * @param nodeId the id of the node in question
         * @return a vector of node ids
         */
    std::vector<WorkerId> getRemovedChildren(WorkerId nodeId) const;

  private:
    /**
         * @brief removes the links represented by an edge from a map which has the parent as a key and a vector of children as the
         * value type and removes the whole entry if the vector becomes empty after removing the last child
         * @param map the mop from which values are to be removed
         * @param edges the list of links to remove from the map
         */
    static void removeLinksFromMap(std::unordered_map<WorkerId, std::vector<WorkerId>>& map, const std::vector<TopologyLinkInformation>& edges);

    /**
         * @brief Helper function that adds the elements in the vectors contained in newMap to the vectors found under the same key in additionTarget, but
         * only if the element is not contained in the vector found under the same key in toSubtract. This helper function is used
         * to update the lists of added or removed links when a new changelog is added to an existing one.
         * @param newMap map containing vectors with elements to add to the corresponding entries in addition target, the
         * calle has to ensure that this does not contain any edges that are already present at the target
         * @param additionTarget elements from newMap get inserted into the vectors in this map
         * @param toSubtract if an element is present here, it will not get added to additionTarget even if it is present in newMap
         * but any element present in newMap will be removed from toSubtract
         */
    static void updateChangelog(const std::unordered_map<WorkerId, std::vector<WorkerId>>& newMap,
                                std::unordered_map<WorkerId, std::vector<WorkerId>>& additionTarget,
                                std::unordered_map<WorkerId, std::vector<WorkerId>>& toSubtract);

    std::unordered_map<WorkerId, std::vector<WorkerId>> addedLinks;
    std::unordered_map<WorkerId, std::vector<WorkerId>> removedLinks;
};
}// namespace Experimental::TopologyPrediction
}// namespace NES
#endif// NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_PREDICTION_TOPOLOGYCHANGELOG_HPP_
