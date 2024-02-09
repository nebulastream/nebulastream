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
#ifndef NES_COMMON_INCLUDE_UTIL_TOPOLOGYLINKINFORMATION_HPP_
#define NES_COMMON_INCLUDE_UTIL_TOPOLOGYLINKINFORMATION_HPP_
#include <cstdint>
#include <string>

namespace NES {
using WorkerId = uint64_t;
/**
 * @brief this class represents a topology edge between 2 nodes
 */
class TopologyLinkInformation {
  public:
    TopologyLinkInformation(WorkerId upstreamTopologyNode, WorkerId downstreamTopologyNode);

    /**
     * @brief get a string representation of this edge
     * @return a string in the format "UPSTREAM_ID->DOWNSTREAM_ID"
     */
    [[nodiscard]] std::string toString() const;

    /**
     * @brief Two edges are equal if they have the same child and the same parent
     * @param other the edge to compare this edge with
     * @return true if the edges are equal
     */
    bool operator==(const TopologyLinkInformation& other) const;

    WorkerId upstreamTopologyNode;
    WorkerId downstreamTopologyNode;
};
}// namespace NES
#endif // NES_COMMON_INCLUDE_UTIL_TOPOLOGYLINKINFORMATION_HPP_
