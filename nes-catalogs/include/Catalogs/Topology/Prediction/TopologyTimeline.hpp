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
#ifndef TOPOLOGYPREDICTION__TOPOLOGYTIMELINE_HPP_
#define TOPOLOGYPREDICTION__TOPOLOGYTIMELINE_HPP_
#include <Catalogs/Topology/Prediction/TopologyChangeLog.hpp>
#include <absl/container/btree_map.h>
#include <memory>

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Experimental::TopologyPrediction {
using Timestamp = uint64_t;
class TopologyDelta;
class TopologyChangeLog;
class TopologyTimeline;
using TopologyTimelinePtr = std::shared_ptr<TopologyTimeline>;

/**
 * This class processes predicted topology changes and allows inspecting the exepcted future states of the topology based on those
 * predictions. Predictions can be added by supplying a TopologyDelta containing the expected link changes for a certain point in
 * time. Predictions can be removed by supplying a delta containing all link changes that are to be removed. Topology versions for
 * a specified time are created by making a copy of the existing topology and applying all changes expected to happen before the
 * specified time onto the copy.
 */
class TopologyTimeline {
  public:
    /**
     * @brief constructor
     * @param originalTopology a pointer to the original topology
     */
    explicit TopologyTimeline(TopologyPtr originalTopology);

    /**
     * @brief create a new topology timeline
     * @param originalTopology a pointer to the original topology
     * @return a pointer to the newly created object
     */
    static TopologyTimelinePtr create(TopologyPtr originalTopology);

    /**
     * @brief adds a topology delta to the changes expected to happen at a specific point in time
     * @param predictedTime the time at which the changes are expected to happen
     * @param delta a delta containing the edges to be removed or added
     */
    void addTopologyDelta(Timestamp predictedTime, const TopologyDelta& delta);

    /**
     * @brief return a new topology object which represents the expected state of the topology at the specified time
     * @param time the point in time for which the expected state should be returned
     * @return a copy of the original topology onto which all changes, which are expected to happen before the specified time,
     * have been applied
     */
    TopologyPtr getTopologyVersion(Timestamp time);

    /**
     * @brief removes a topology delta from the changes expected to happen at a specific point in time
     * @param predictedTime the time at which the changes are expected to happen
     * @param delta a delta containing the edges to be removed or added
     */
    bool removeTopologyDelta(Timestamp predictedTime, const TopologyDelta& delta);

  private:
    /**
     * @brief create a new topology representing the state which would result if all changes in the changelog were applied onto
     * the original topology
     * @param changeLog a change log containing the changes to be applied
     * @return a pointer to the new topology object onto which the changes are applied
     */
    TopologyPtr createTopologyVersion(const TopologyChangeLog& changeLog);

    /**
     * @brief remove all changes expected to happen at the specified time
     * @param time the time for which the changelog is to be removed
     */
    void removeTopologyChangeLogAt(Timestamp time);

    /**
     * @brief creates an aggregated changelog representing the difference between the original topology and the expected state at
     * the specified time
     * @param time all changelogs which are less or equal to this time will be included in the aggregate
     * @return an aggregated changelog
     */
    TopologyChangeLog createAggregatedChangeLog(Timestamp time);

    TopologyPtr originalTopology;
    absl::btree_map<Timestamp, TopologyChangeLog> changeMap;
};
}// namespace Experimental::TopologyPrediction
}// namespace NES
#endif//TOPOLOGYPREDICTION__TOPOLOGYTIMELINE_HPP_