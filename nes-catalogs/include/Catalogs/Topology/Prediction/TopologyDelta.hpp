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
#ifndef NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_PREDICTION_TOPOLOGYDELTA_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_PREDICTION_TOPOLOGYDELTA_HPP_
#include <cstdint>
#include <memory>
#include <vector>

namespace NES {
class TopologyLinkInformation;

namespace Experimental::TopologyPrediction {
    /**
 * @brief this class represents a set of changes to be applied to the topology changelog for a certain point in time. It consists
 * of prediction from one or more nodes.
 */
    class TopologyDelta {
      public:
        /**
   * @brief constructor
   * @param added a list of edges to be added to the topology. It is the callers responsibility to ensure that this list does not
   * contain any duplicates
   * @param removed a list of edges to be removed from the topology. It is the callers responsibility to ensure that this list does not
   * contain any duplicates
   */
        TopologyDelta(std::vector<TopologyLinkInformation> added, std::vector<TopologyLinkInformation> removed);

        /**
     * @brief this function returns a string visualizing the content of the changelog
     * @return a string in the format "added: {CHILD->PARENT, CHILD->PARENT, ...} removed: {CHILD->PARENT, CHILD->PARENT, ...}"
     */
        [[nodiscard]] std::string toString() const;

        /**
     * @brief return the list of added edges contained in this delta
     * @return a const reference to a vector of edges
     */
        [[nodiscard]] const std::vector<TopologyLinkInformation>& getAdded() const;

        /**
     * @brief return the list of removed edges contained in this delta
     * @return a const reference to a vector of edges
     */
        [[nodiscard]] const std::vector<TopologyLinkInformation>& getRemoved() const;

      private:
        /**
     * @brief obtain a string representation of a vector of edges
     * @param edges a vector of edges
     * @return a string in the format "CHILD->PARENT, CHILD->PARENT, ..."
     */
        static std::string edgeListToString(const std::vector<TopologyLinkInformation>& edges);

        std::vector<TopologyLinkInformation> added;
        std::vector<TopologyLinkInformation> removed;
    };
}// namespace NES::Experimental::TopologyPrediction
}// namespace NES
#endif// NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_PREDICTION_TOPOLOGYDELTA_HPP_
