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

#include <Optimizer/RequestTypes/TopologyRequests/MigrateTopologyNodeRequest.hpp>
#include <sstream>

namespace NES::Experimental {

MigrateTopologyNodeRequest::MigrateTopologyNodeRequest(std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToRemove,
                                                       std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToAdd)
    : linksToRemove(std::move(linksToRemove)), linksToAdd(std::move(linksToAdd)) {}

MigrateTopologyNodeRequestPtr
MigrateTopologyNodeRequest::create(std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToRemove,
                                   std::vector<std::pair<TopologyNodeId, TopologyNodeId>> linksToAdd) {
    return std::make_shared<MigrateTopologyNodeRequest>(
        MigrateTopologyNodeRequest(std::move(linksToRemove), std::move(linksToAdd)));
}

const std::vector<std::pair<TopologyNodeId, TopologyNodeId>>& MigrateTopologyNodeRequest::getLinksToRemove() const {
    return linksToRemove;
}

const std::vector<std::pair<TopologyNodeId, TopologyNodeId>>& MigrateTopologyNodeRequest::getLinksToAdd() const {
    return linksToAdd;
}

std::string MigrateTopologyNodeRequest::toString() {
    std::stringstream ss;
    ss << "MigrateTopologyNodeRequest { LinksToRemove [ ";
    for (const auto& item : linksToRemove) {
        ss << "{" + std::to_string(item.first) + ", " + std::to_string(item.second) + "}" << std::endl;
    }
    ss << "]" << std::endl << "LinksToAdd [ ";
    for (const auto& item : linksToAdd) {
        ss << "{" + std::to_string(item.first) + ", " + std::to_string(item.second) + "}" << std::endl;
    }
    ss << "] }";
    return ss.str();
}

RequestType MigrateTopologyNodeRequest::getRequestType() { return RequestType::MigrateTopologyNode; }

}// namespace NES::Experimental
