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
#include <sstream>
#include <utility>
#include <Catalogs/Topology/Prediction/TopologyDelta.hpp>
#include <Util/TopologyLinkInformation.hpp>

namespace NES::Experimental::TopologyPrediction
{
TopologyDelta::TopologyDelta(std::vector<TopologyLinkInformation> added, std::vector<TopologyLinkInformation> removed)
    : added(std::move(added)), removed(std::move(removed))
{
}

std::string TopologyDelta::toString() const
{
    std::stringstream deltaString;

    deltaString << "added: {";
    deltaString << topologyLinkInformationListToString(added);
    deltaString << "}, ";

    deltaString << "removed: {";
    deltaString << topologyLinkInformationListToString(removed);
    deltaString << "}";

    return deltaString.str();
}

std::string TopologyDelta::topologyLinkInformationListToString(const std::vector<TopologyLinkInformation>& topologyLinks)
{
    std::stringstream deltaString;

    if (topologyLinks.empty())
    {
        return "";
    }

    //add first item to string
    auto addedIter = topologyLinks.cbegin();
    deltaString << addedIter->toString();
    ++addedIter;

    //add comma separated additional items
    for (; addedIter != topologyLinks.cend(); ++addedIter)
    {
        deltaString << ", ";
        deltaString << addedIter->toString();
    }

    return deltaString.str();
}

const std::vector<TopologyLinkInformation>& TopologyDelta::getAdded() const
{
    return added;
}

const std::vector<TopologyLinkInformation>& TopologyDelta::getRemoved() const
{
    return removed;
}
} // namespace NES::Experimental::TopologyPrediction
