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

#ifndef NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_TOPOLOGYREQUESTS_MIGRATETOPOLOGYNODEREQUEST_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_TOPOLOGYREQUESTS_MIGRATETOPOLOGYNODEREQUEST_HPP_

#include <RequestTypes/Request.hpp>
#include <vector>

namespace NES::Experimental {

class MigrateTopologyNodeRequest;
using MigrateTopologyNodeRequestPtr = std::shared_ptr<MigrateTopologyNodeRequest>;

/**
 * @brief This request is used for migrating a topology node from one location to another by removing some existing links and
 * adding some new links to the topology
 */
class MigrateTopologyNodeRequest : public Request {
  public:
    /**
     * @brief Create an instance of migrate topology node request
     * @param linksToRemove : vector of links to be removed
     * @param linksToAdd : vector of links to be added
     * @return a shared pointer to the migrate topology node request
     */
    static MigrateTopologyNodeRequestPtr create(std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToRemove,
                                                std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToAdd);

    /**
     * @brief Get all links that were removed
     * @return returns all links that need to be removed
     */
    const std::vector<std::pair<TopologyNodeId, TopologyNodeId>>& getLinksToRemove() const;

    /**
     * @brief Get all links that are added
     * @return return all new links
     */
    const std::vector<std::pair<TopologyNodeId, TopologyNodeId>>& getLinksToAdd() const;

    std::string toString() override;

    RequestType getRequestType() override;

  private:
    explicit MigrateTopologyNodeRequest(std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToRemove,
                                        std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToAdd);

    std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToRemove;
    std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToAdd;
};
}// namespace NES::Experimental
#endif// NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_TOPOLOGYREQUESTS_MIGRATETOPOLOGYNODEREQUEST_HPP_
