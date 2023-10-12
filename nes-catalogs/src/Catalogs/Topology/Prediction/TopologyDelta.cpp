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
#include <Catalogs/Topology/Prediction/Edge.hpp>
#include <Catalogs/Topology/Prediction/TopologyDelta.hpp>
#include <sstream>
#include <utility>

namespace NES::Experimental::TopologyPrediction {
TopologyDelta::TopologyDelta(std::vector<Edge> added, std::vector<Edge> removed)
    : added(std::move(added)), removed(std::move(removed)) {}

std::string TopologyDelta::toString() const {
    std::stringstream deltaString;

    deltaString << "added: {";
    deltaString << edgeListToString(added);
    deltaString << "}, ";

    deltaString << "removed: {";
    deltaString << edgeListToString(removed);
    deltaString << "}";

    return deltaString.str();
}

std::string TopologyDelta::edgeListToString(const std::vector<Edge>& edges) {
    std::stringstream deltaString;

    if (edges.empty()) {
        return "";
    }

    //add first item to string
    auto addedIter = edges.cbegin();
    deltaString << addedIter->toString();
    ++addedIter;

    //add comma separated additional items
    for (; addedIter != edges.cend(); ++addedIter) {
        deltaString << ", ";
        deltaString << addedIter->toString();
    }

    return deltaString.str();
}

const std::vector<Edge>& TopologyDelta::getAdded() const { return added; }

const std::vector<Edge>& TopologyDelta::getRemoved() const { return removed; }
}// namespace NES::Experimental::TopologyPrediction
