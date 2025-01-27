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

#ifndef NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKER_HPP_
#define NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKER_HPP_

#include <Identifiers/DecomposedQueryIdWithVersion.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/ReconfigurationMarkerEvent.hpp>
#include <optional>
#include <unordered_map>

namespace NES {

class ReconfigurationMarker;
using ReconfigurationMarkerPtr = std::shared_ptr<ReconfigurationMarker>;

/**
 * @brief This class represents a reconfiguration marker that passes through currently deployed versions of decomposed query plans
 * and transforms them. To this end, a reconfiguration marker contains a collection of instructions for different decomposed
 * query plans to allow the transformation. This information is wrapped together in a map.
 *
 * The following information is provided within a reconfiguration marker:
 *
 * Key (Who): represents identity of the decomposed query plan that needs to react upon receiving the reconfiguration marker.
 *            The key is composed of WorkerId, SharedQueryId, and DecomposedQueryId.
 *
 * State (What): represents what need to be done upon receiving the reconfiguration marker. Currently we support: (1.) Draining
 * a decomposed plan (terminate a plan), (2.) Updating a decomposed plan (plan mutation), and (3.) Updating and then draining a
 * decomposed plan (plan termination post state migration).
 *
 * Reconfiguration Metadata (How): represent necessary information on how to perform the reconfiguration.
 *
 * The "What" and "How" are represented together as ReconfigurationMarkerEvent.
 */
class ReconfigurationMarker {
  public:
    static ReconfigurationMarkerPtr create();
    ReconfigurationMarker() = default;

    /**
     * @brief Get the reconfiguration marker event
     * @param decomposedQueryId : the id and version for which the reconfiguration marker needs to be defined
     * @return optional immutable reconfiguration marker event
     */
    std::optional<ReconfigurationMarkerEventPtr>
    getReconfigurationEvent(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion) const;

    /**
     * @brief Get the reconfiguration events for all versions of the plan with the supplied id
     * @param decomposedQueryId : the id for which the reconfiguration events are to be retrieved
     * @return optional immutable reconfiguration marker event
     */
    std::vector<ReconfigurationMarkerEventPtr> getReconfigurationEventsForPlanId(DecomposedQueryId decomposedQueryId) const;

    /**
     * @brief Add a reconfiguration marker event
     * @param decomposedQueryId : key identifying the decomposed query plan id
     * @param decomposedQueryVersion : key identifying the decomposed query plan version
     * @param reconfigurationEvent : the reconfiguration marker event
     */
    void addReconfigurationEvent(DecomposedQueryId decomposedQueryId,
                                 DecomposedQueryPlanVersion decomposedQueryVersion,
                                 const ReconfigurationMarkerEventPtr reconfigurationEvent);

    /**
     * @brief Add a reconfiguration marker event
     * @param decomposedQueryId : key identifying the decomposed query plan id and version
     * @param reconfigurationEvent : the reconfiguration marker event
     */
    void addReconfigurationEvent(DecomposedQueryIdWithVersion decomposedQueryIdWithVersion,
                                 const ReconfigurationMarkerEventPtr reconfigurationEvent);

    /**
     * @brief Get all reconfiguration marker events defined in the reconfiguration marker
     */
    const std::unordered_map<DecomposedQueryIdWithVersion, ReconfigurationMarkerEventPtr>&
    getAllReconfigurationMarkerEvents() const;

  private:
    std::unordered_map<DecomposedQueryIdWithVersion, ReconfigurationMarkerEventPtr> reconfigurationEvents;
};

}// namespace NES

#endif// NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKER_HPP_
